#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include "session_mux.h"
using asio::ip::udp;
using SessionMux = p2psocks::SessionMux;
using Session = p2psocks::Session;

class UdpSession : public std::enable_shared_from_this<UdpSession>
{

public:
    UdpSession(asio::io_context &io_context, SessionMux &mux, std::shared_ptr<Session> session, uint32_t session_id)
        : io_(io_context), mux_(mux), session_(session), recv_buf_(65535), socket_(io_context), session_id_(session_id)
    {
        PLOG_DEBUG << "UdpSession created, session_id: " << session_id;
    }

    ~UdpSession()
    {
        PLOG_DEBUG << "UdpSession destroyed, session_id: " << session_id_;
    }

    void close()
    {
        PLOG_DEBUG << "UdpSession close, session_id: " << session_id_;
        socket_.close();
    }

    bool start()
    {
        asio::error_code ec;

        socket_.open(udp::v4(), ec);
        if (ec)
        {
            PLOG_ERROR
                << "open failed:"
                << ec.message()
                << ", session_id: " << session_id_;
            return false;
        }
        socket_.bind(udp::endpoint(udp::v4(), 0), ec);
        if (ec)
        {
            PLOG_ERROR
                << "bind failed:"
                << ec.message()
                << ", session_id: " << session_id_;
            return false;
        }

        PLOG_DEBUG << "UDP server listening on port " << socket_.local_endpoint().port() << ", session_id: " << session_id_;

        // 回给客户端的数据
        std::weak_ptr<UdpSession> weak_self = shared_from_this();

        session_->set_on_udp_data([weak_self](const std::string &host, uint16_t port,
                                              std::shared_ptr<std::vector<uint8_t>> data)
                                  {
        if (weak_self.expired())
        {
            PLOG_WARNING << "set_on_udp_data UdpSession expired";
            return;
        }
        auto self = weak_self.lock();
        auto reply = std::make_shared<std::vector<uint8_t>>();

        reply->push_back(0x00); // RSV (high byte)
        reply->push_back(0x00); // RSV (low byte)
        reply->push_back(0x00); // FRAG
        reply->push_back(0x01); // ATYP IPv4

        // 简单示例：IPv4
        asio::ip::address_v4 addr =
            asio::ip::make_address_v4(host);

        auto bytes = addr.to_bytes();

        reply->insert(reply->end(),
                      bytes.begin(),
                      bytes.end());

        reply->push_back((port >> 8) & 0xff);
        reply->push_back(port & 0xff);

        reply->insert(reply->end(), data->begin(), data->end());
        
        self->send(reply); });

        start_receive();
        return true;
    }

    int getLocalPort()
    {
        return socket_.local_endpoint().port();
    }

    std::string get_local_ip()
    {
        asio::ip::udp::socket sock(io_);
        sock.open(asio::ip::udp::v4());

        sock.connect(
            asio::ip::udp::endpoint(
                asio::ip::make_address("114.114.114.114"),
                53));

        return sock.local_endpoint().address().to_string();
    }

private:
    void start_receive()
    {
        auto self(shared_from_this());
        socket_.async_receive_from(
            asio::buffer(recv_buf_), remote_endpoint_,
            [self](std::error_code ec, std::size_t bytes_recvd)
            {
                if (ec)
                {
                    PLOG_ERROR << "receive error: " << ec.message();
                    return;
                }

                if (bytes_recvd <= 0)
                {
                    PLOG_WARNING << "invalid packet bytes_recvd <= 0, stream_id:"<<self->session_->stream_id();
                    return;
                }

                if (!self->client_known_)
                {
                    // 第一次收到包,记录这个客户端地址
                    self->client_endpoint_ = self->remote_endpoint_;
                    self->client_known_ = true;
                    PLOG_INFO << "first packet, record client endpoint stream_id:"<<self->session_->stream_id() << " " << self->client_endpoint_.address().to_string() << ":" << self->client_endpoint_.port();
                }
                else if (self->remote_endpoint_.address() != self->client_endpoint_.address())
                {
                    // 源 IP 不一致,按协议要求丢弃(防止伪造)
                    PLOG_WARNING << "source ip changed, reject packet stream_id:"<<self->session_->stream_id();
                    self->start_receive();
                    return;
                }
                else if (self->remote_endpoint_.port() != self->client_endpoint_.port())
                {
                    // 端口变了但 IP 一致,按你的策略选择更新还是拒绝
                    PLOG_INFO << "port changed, update client endpoint stream_id:"<<self->session_->stream_id();
                    self->client_endpoint_ = self->remote_endpoint_; // 选择宽松处理:更新端口
                }

                if (self->recv_buf_[3] == 0x01)
                {
                    // 处理 IPv4 连接
                    self->send_ipv4(bytes_recvd, self->remote_endpoint_.address().to_string(), self->remote_endpoint_.port());
                }
                else if (self->recv_buf_[3] == 0x03)
                {
                    self->send_domain(bytes_recvd, self->remote_endpoint_.address().to_string(), self->remote_endpoint_.port());
                }
                else
                {
                    PLOG_ERROR << "invalid network type stream_id:"<<self->session_->stream_id();
                }
                // 继续监听下一个包
                self->start_receive();
            });
    }

    void send_ipv4(std::size_t bytes_recvd, const std::string &local_host, uint16_t local_port)
    {
        if (bytes_recvd < 10)
        {
            PLOG_WARNING << "invalid ipv4 packet stream_id:"<<session_->stream_id();
            return;
        }
        char tmp[32];
        std::snprintf(tmp, sizeof(tmp), "%d.%d.%d.%d", recv_buf_[4], recv_buf_[5],
                      recv_buf_[6], recv_buf_[7]);
        std::string target_host = tmp;
        int target_port = (uint16_t(recv_buf_[8]) << 8) | recv_buf_[9];
        std::vector<uint8_t> data(recv_buf_.begin() + 10, recv_buf_.begin() + bytes_recvd);

        mux_.send_udp(session_->stream_id(), target_host, target_port, data);
    }

    void send_domain(std::size_t bytes_recvd, const std::string &local_host, uint16_t local_port)
    {
        if (bytes_recvd < 5)
        {
            PLOG_WARNING << "invalid domain packet stream_id:"<<session_->stream_id();
            return;
        }
        int len = recv_buf_[4];
        if (5 + len + 2 > (int)bytes_recvd)
        {
            PLOG_WARNING << "invalid domain packet 2 stream_id:"<<session_->stream_id();
            return;
        }

        std::string target_host(recv_buf_.begin() + 5, recv_buf_.begin() + 5 + len);
        int target_port = (uint16_t(recv_buf_[6 + len]) << 8) | recv_buf_[7 + len];
        std::vector<uint8_t> data(recv_buf_.begin() + 8 + len, recv_buf_.begin() + bytes_recvd);

        mux_.send_udp(session_->stream_id(), target_host, target_port, data);
    }
    // 发送回客户端
    void send(std::shared_ptr<std::vector<uint8_t>> data)
    {
        send_queue_.push_back(std::move(data));
        if (!sending_)
        {
            do_send_next();
            sending_ = true;
        }
    }

    void do_send_next()
    {
        auto data = send_queue_.front();
        /*auto target_endpoint = asio::ip::udp::endpoint(
            asio::ip::make_address(send_data.target_host),
            send_data.target_port);*/
        auto self = shared_from_this();
        socket_.async_send_to(
            asio::buffer(*data),
            client_endpoint_,
            [self](std::error_code ec, std::size_t bytes_sent)
            {
                if (ec)
                {
                    PLOG_ERROR << "send error: "
                              << ec.message() << " stream_id:"<<self->session_->stream_id();
                    return;
                }
                self->send_queue_.pop_front();
                if (!self->send_queue_.empty())
                {
                    self->do_send_next();
                }
                else
                {
                    self->sending_ = false;
                }
            });
    }

    asio::io_context &io_;
    SessionMux &mux_;
    std::shared_ptr<Session> session_;
    std::vector<uint8_t> recv_buf_;
    udp::socket socket_;
    udp::endpoint remote_endpoint_;
    udp::endpoint client_endpoint_;
    std::deque<std::shared_ptr<std::vector<uint8_t>>> send_queue_;
    bool sending_ = false;
    bool client_known_ = false;
    uint32_t session_id_;
};