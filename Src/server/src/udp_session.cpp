#include "udp_session.h"
#include <plog/Log.h>
#include <asio.hpp>

using SendData = p2psocks::SendData;

UdpClient::UdpClient(asio::io_context &io_context, std::weak_ptr<SessionMux> weak_mux, uint32_t session_id)
    : io_(io_context)
    ,socket_(io_context)
    , weak_mux_(weak_mux)
    , recv_buf_(65536)
{
    if(weak_mux_.expired())
    {
        PLOG_ERROR << "weak_mux is expired";
        return;
    }
    else
    {
        session_ = weak_mux_.lock()->create_session(session_id);
    }
    PLOG_DEBUG << "UdpClient created, session_id: " << session_id;
}

UdpClient::~UdpClient()
{
    PLOG_DEBUG << "UdpClient destroyed";
    if (session_)
    {
        if(weak_mux_.expired())
        {
            PLOG_ERROR << "weak_mux is expired";
            return;
        }
        else
        {
            weak_mux_.lock()->remove_session(session_->stream_id());
        }
    }
}

bool UdpClient::start()
{
    asio::error_code ec;

    socket_.open(udp::v4(), ec);
    if (ec)
    {
        PLOG_ERROR
            << "open failed:"
            << ec.message();
        if(weak_mux_.expired())
        {
            PLOG_ERROR << "weak_mux is expired";
            return false;
        }
        else
        {
            weak_mux_.lock()->send_udp_synack(session_->stream_id(), false);
        }
        close();
        return false;
    }
    socket_.bind(udp::endpoint(udp::v4(), 0), ec);
    if (ec)
    {
        PLOG_ERROR
            << "bind failed:"
            << ec.message();
        if(weak_mux_.expired())
        {
            PLOG_ERROR << "weak_mux is expired";
            return false;
        }
        else
        {
            weak_mux_.lock()->send_udp_synack(session_->stream_id(), false);
        }
        close();
        return false;
    }
    PLOG_DEBUG << "UDP server listening on port " << socket_.local_endpoint().port() << ", session_id: " << session_id_;

    start_receive();

    // 客户端来的数据
    std::weak_ptr<UdpClient> weak_self = shared_from_this();

    session_->set_on_udp_data([weak_self](const std::string &remote_host, uint16_t remote_port, std::shared_ptr<std::vector<uint8_t>> data)
                              { 
                                if(weak_self.expired())
                                {
                                    return;
                                }
                                auto self = weak_self.lock();
                                
                                self->send(data, remote_host, remote_port); });

    session_->set_on_udp_close([weak_self]()
                               {
                                 if(weak_self.expired())
                                 {
                                     return;
                                 }
                                 auto self = weak_self.lock();
                                 self->close();
                                 PLOG_DEBUG << "UdpClient close, session_id: " << self->session_id_; });
    if(weak_mux_.expired())
    {
        PLOG_ERROR << "weak_mux is expired";
        return false;
    }
    else
    {
        weak_mux_.lock()->send_udp_synack(session_->stream_id(), true);
    }

    return true;
}

void UdpClient::bind_close_func(std::function<void(uint32_t session_id)> close_func)
{
    this->close_func_ = std::move(close_func);
}

void UdpClient::set_session_id(int32_t session_id)
{
    this->session_id_ = session_id;
}

void UdpClient::send(std::shared_ptr<std::vector<uint8_t>> data, const std::string &target_host, int target_port)
{
    p2psocks::SendData send_data = {std::move(target_host), std::move(target_port), std::move(data)};
    send_queue_.push_back(std::move(send_data));
    if (!sending_)
    {
        do_send_next();
        sending_ = true;
    }
}

void UdpClient::do_send_next()
{
    SendData send_data = send_queue_.front();

    auto self(shared_from_this());
    auto resolver = std::make_shared<udp::resolver>(io_);
    resolver->async_resolve(
        send_data.target_host, std::to_string(send_data.target_port),
        [this, self, resolver, data = send_data.data](std::error_code ec,
                                                      udp::resolver::results_type results)
        {
            if (ec)
            {
                PLOG_ERROR << "resolve host failed " << ec.message();
                send_queue_.pop_front();
                if (!send_queue_.empty())
                    do_send_next();
                else
                    sending_ = false;
                return;
            }

            //auto endpoint = results.begin()->endpoint();

            // 关键日志：打印实际解析出来的地址、端口、协议族
            //PLOG_INFO << "resolved endpoint: "
            //          << endpoint.address().to_string()
            //          << ":" << endpoint.port()
            //          << " (is_v4=" << endpoint.address().is_v4()
            //          << ", is_v6=" << endpoint.address().is_v6() << ")";
//
            //PLOG_INFO << "send data size: " << data->size();

            socket_.async_send_to(
                asio::buffer(*data),
                results.begin()->endpoint(),
                [self, data](std::error_code ec, std::size_t bytes_sent)
                {
                    if (ec)
                    {
                        PLOG_ERROR << "send error: "
                                   << ec.message();
                        self->close();
                        return;
                    }
                    //PLOG_INFO << "send success, bytes_sent: " << bytes_sent;
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
        });
}

void UdpClient::start_receive()
{
    auto self = shared_from_this();

    socket_.async_receive_from(
        asio::buffer(recv_buf_), sender_endpoint_,
        [self](std::error_code ec, std::size_t bytes_recvd)
        {
            if (!ec)
            {
                std::string host = self->sender_endpoint_.address().to_string();
                uint16_t port = self->sender_endpoint_.port();

                std::vector<uint8_t> reply(self->recv_buf_.begin(), self->recv_buf_.begin() + bytes_recvd);
                if(self->weak_mux_.expired())
                {
                    PLOG_ERROR << "weak_mux is expired";
                    return;
                }
                else
                {
                    self->weak_mux_.lock()->send_udp(self->session_->stream_id(), host, port, reply);
                }
                self->start_receive();
            }
            else
            {
                PLOG_ERROR << "receive error: " << ec.message();
                self->close();
            }
        });
}

void UdpClient::close()
{
     {
        std::lock_guard<std::mutex> lock(mutex_);

        if (is_closed_)
            return;

        is_closed_ = true;
    }

    std::error_code ec;
    socket_.close(ec);

    if (ec)
    {
        PLOG_ERROR << "socket close error: " << ec.message()
                   << ", value=" << ec.value();
    }

    if (close_func_)
    {
        close_func_(session_id_);
    }
}