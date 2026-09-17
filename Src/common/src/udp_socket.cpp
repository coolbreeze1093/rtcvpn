#include "udp_socket.h"
#include <plog/Log.h>

using asio::ip::udp;
using namespace p2psocks;

UdpSocket::UdpSocket(asio::io_context &io)
    : io_(io), strand_(asio::make_strand(io)), socket_(strand_), recv_buf_(65536)
{
    PLOG_DEBUG << "UdpSession created";
}

UdpSocket::~UdpSocket()
{
    PLOG_DEBUG << "UdpSocket destroyed";
    send_queue_.clear();
}

void UdpSocket::setOpenCallback(OpenCallback cb)
{
    open_callback_ = std::move(cb);
}

void UdpSocket::setDataCallback(DataCallback cb)
{
    data_callback_ = std::move(cb);
}

void UdpSocket::setCloseCallback(CloseCallback cb)
{
    close_callback_ = std::move(cb);
}

void p2psocks::UdpSocket::setWriteQueueCallback(WriteQueueCallback cb)
{
    write_queue_callback_ = std::move(cb);
}

void UdpSocket::start(uint16_t local_port)
{
    auto self(shared_from_this());
    asio::post(strand_, [this, self, local_port]()
    {
        std::error_code ec;

        socket_.open(udp::v4(), ec);
        if (ec)
        {
            PLOG_ERROR << "udp open failed: " << ec.message();
            if (open_callback_)
            {
                open_callback_(false);
            }
            return;
        }

        socket_.bind(udp::endpoint(udp::v4(), local_port), ec);
        if (ec)
        {
            PLOG_ERROR << "udp bind failed: " << ec.message();
            if (open_callback_)
            {
                open_callback_(false);
            }
            return;
        }

        PLOG_DEBUG << "UdpSocket listening on port " << socket_.local_endpoint().port();

        is_open_ = true;
        if (open_callback_)
        {
            open_callback_(true);
        }
        do_read();
    });
}

void UdpSocket::send(std::shared_ptr<std::vector<uint8_t>> data,
                       const std::string &target_host, uint16_t target_port)
{
    auto self(shared_from_this());
    asio::post(strand_, [this, self, data = std::move(data), target_host, target_port]() mutable
    {
        if (!is_open_ || is_closing_ || is_closed_)
        {
            PLOG_ERROR << "send called before open or after close, data dropped";
            return;
        }

        send_queue_.push_back(PendingSend{target_host, target_port, std::move(data)});
        if (!is_sending_)
        {
            is_sending_ = true;
            do_send_next();
        }
    });
}

void UdpSocket::send(const uint8_t *data, size_t len,
                       const std::string &target_host, uint16_t target_port)
{
    send(std::make_shared<std::vector<uint8_t>>(data, data + len), target_host, target_port);
}

void UdpSocket::close()
{
    auto self(shared_from_this());
    asio::post(strand_, [this, self]()
    {
        if (is_closing_ || is_closed_)
        {
            return;
        }
        is_closing_ = true;

        // 尚未打开成功时，没有数据需要发送，直接关闭
        if (!is_open_)
        {
            do_close();
            return;
        }

        // 如果当前没有正在发送、且队列为空，说明没有数据要"发完"，直接关闭
        if (!is_sending_ && send_queue_.empty())
        {
            do_close();
        }
        // 否则等 do_send_next() 把队列发完后，会检测到 is_closing_ 并调用 do_close()
    });
}

void UdpSocket::startReading()
{
    auto self(shared_from_this());

    asio::post(strand_, [this, self]()
    {
        if (is_reading_ || is_closing_ || is_closed_)
        {
            return;
        }

        is_reading_ = true;

        if(is_open_)
        {
            do_read();
        }
        
    });
}

void UdpSocket::stopReading()
{
    auto self(shared_from_this());

    asio::post(strand_, [this, self]()
    {
        is_reading_ = false;
    });
}

int p2psocks::UdpSocket::getLocalPort()
{
    return socket_.local_endpoint().port();
}

std::string p2psocks::UdpSocket::get_local_ip()
{
    asio::ip::udp::socket sock(io_);
    sock.open(asio::ip::udp::v4());

    sock.connect(
        asio::ip::udp::endpoint(
            asio::ip::make_address("114.114.114.114"),
            53));

    return sock.local_endpoint().address().to_string();
}

void UdpSocket::do_send_next()
{
    // do_send_next 只应该在队列非空时被调用
    const PendingSend &item = send_queue_.front();

    auto self(shared_from_this());
    auto resolver = std::make_shared<udp::resolver>(strand_);
    resolver->async_resolve(
        item.target_host, std::to_string(item.target_port),
        [this, self, resolver](std::error_code ec, udp::resolver::results_type results)
        {
            if (ec)
            {
                PLOG_ERROR << "resolve target failed: " << ec.message();
                send_queue_.pop_front();
                if (!send_queue_.empty())
                {
                    do_send_next();
                    return;
                }
                is_sending_ = false;
                if (is_closing_)
                {
                    do_close();
                }
                return;
            }

            auto data = send_queue_.front().data;
            socket_.async_send_to(
                asio::buffer(*data), results.begin()->endpoint(),
                [this, self, data](std::error_code ec, std::size_t /*bytes_sent*/)
                {
                    if (ec)
                    {
                        PLOG_ERROR << "udp send error: " << ec.message();
                        is_closing_ = true;
                        do_close();
                        return;
                    }

                    send_queue_.pop_front();
                    if (!send_queue_.empty())
                    {
                        do_send_next();
                        return;
                    }

                    is_sending_ = false;

                    // 队列已经发送完毕；如果外部已经请求关闭，此时真正关闭
                    if (is_closing_)
                    {
                        do_close();
                    }
                });
        });
}

void UdpSocket::do_read()
{
    auto self(shared_from_this());
    socket_.async_receive_from(
        asio::buffer(recv_buf_), sender_endpoint_,
        [this, self](std::error_code ec, std::size_t bytes_recvd)
        {
            if (ec)
            {
                PLOG_ERROR << "udp receive error: " << ec.message();
                // 对端/网络出错，不再需要保证队列发送，直接关闭
                is_closing_ = true;
                do_close();
                return;
            }

            if (data_callback_)
            {
                data_callback_(sender_endpoint_.address().to_string(),
                                sender_endpoint_.port(),
                                recv_buf_.data(), bytes_recvd);
            }
            if (is_reading_)
            {
                do_read();
            }
        });
}

void UdpSocket::do_close()
{
    // do_close() 只会在 strand_ 里被调用（do_send_next/do_read 的
    // handler 本身就绑定在 strand_ 上；close() 也是通过
    // asio::post(strand_, ...) 转进来的），strand 保证同一时刻
    // 只有一个 handler 在跑，因此这里不需要加锁。
    if (is_closed_)
    {
        return;
    }
    is_closed_ = true;

    if (socket_.is_open())
    {
        std::error_code ec;
        socket_.cancel(ec);
        if (ec)
        {
            PLOG_ERROR << "udp socket cancel error: " << ec.message();
        }
        socket_.close(ec);
        if (ec)
        {
            PLOG_ERROR << "udp socket close error: " << ec.message()
                       << ", value=" << ec.value();
        }
    }

    // 只有打开曾经成功过，才触发 CloseCallback；
    // 打开失败的情况由 OpenCallback(false) 表达，避免重复通知。
    if (/* is_open_ &&  */close_callback_)
    {
        close_callback_();
    }
}
