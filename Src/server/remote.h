// remote.cpp —— 远端 (Egress)
// 职责：收到 P2P 隧道传来的 SYN(host,port) -> 真正 connect() 目标服务器
//       -> 把连接结果通过 SYNACK 回给本地端 -> 之后双向转发数据。
//
// !!! 需要你接入的地方，我都用 "TODO(P2P)" 标出了 !!!
//
// 编译:
//   g++ -std=c++17 -O2 -DASIO_STANDALONE remote.cpp -lpthread -o remote
#pragma once
#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include "session_mux.h"
#include <array>

using asio::ip::tcp;
using namespace p2psocks;

class RemoteSession : public std::enable_shared_from_this<RemoteSession>
{
public:
    RemoteSession(asio::io_context &io, SessionMux &mux,
                  uint32_t session_id)
        : io_(io), mux_(mux), target_socket_(io)
    {
        session_ = mux_.create_session(session_id);
    }

    ~RemoteSession()
    {
        if (session_)
        {
            mux_.remove_session(session_->stream_id());
        }
        std::cout << "RemoteSession close, stream_id=" << session_->stream_id() << "\n";
    }

    void bind_close_func(std::function<void(uint32_t session_id)> func)
    {
        close_func_ = func;
    }

    void set_session_id(uint32_t session_id)
    {
        session_id_ = session_id;
    }

    void connect_target(const std::string &host, uint16_t port)
    {
        auto self(shared_from_this());
        auto resolver = std::make_shared<tcp::resolver>(io_);
        resolver->async_resolve(
            host, std::to_string(port),
            [this, self, resolver](std::error_code ec,
                                   tcp::resolver::results_type results)
            {
                if (ec)
                {
                    std::cerr << "resolve host failed " << ec.message() << "\n";
                    mux_.send_synack(session_->stream_id(), false);
                    close();
                    return;
                }
                asio::async_connect(
                    target_socket_, results,
                    [this, self](std::error_code ec, const tcp::endpoint &)
                    {
                        if (ec)
                        {
                            std::cerr << "connect target failed, " << ec.message()
                                      << "\n";
                            mux_.send_synack(session_->stream_id(), false);
                            close();
                            return;
                        }
                        std::cout << "connect target success, stream_id="
                                  << session_->stream_id() << "\n";
                        mux_.send_synack(session_->stream_id(), true);
                        setup_session_callbacks();
                        do_read_from_target();
                    });
            });
    }

private:
    void setup_session_callbacks()
    {
        std::weak_ptr<RemoteSession> weak_self = shared_from_this();

        session_->set_on_data([weak_self](const uint8_t *d, size_t n)
                              {
            // 来自本地端(浏览器)的数据 -> 写给目标服务器
            if(weak_self.expired())
                return;
            auto self = weak_self.lock();

            bool writing = !self->to_target_queue_.empty();
            self->to_target_queue_.emplace_back(d, d + n);
            if (!writing) self->do_write_to_target(); });
        session_->set_on_close([weak_self]()
                               {
            if(weak_self.expired())
                return;
            auto self = weak_self.lock();

            std::error_code ec;
            self->target_socket_.close(ec);
            self->close(); });
    }

    void do_write_to_target()
    {
        auto self(shared_from_this());
        asio::async_write(
            target_socket_, asio::buffer(to_target_queue_.front()),
            [this, self](std::error_code ec, std::size_t)
            {
                if (ec)
                {
                    std::cout << "do_write_to_target error, stream_id=" << session_->stream_id() << std::endl;
                    close();
                    return;
                }
                to_target_queue_.pop_front();
                if (!to_target_queue_.empty())
                    do_write_to_target();
            });
    }

    void do_read_from_target()
    {
        auto self(shared_from_this());
        target_socket_.async_read_some(
            asio::buffer(target_buf_),
            [this, self](std::error_code ec, std::size_t n)
            {
                if (ec)
                {
                    std::cout << "do_read_from_target error, stream_id=" << session_->stream_id() << std::endl;
                    mux_.send_fin(session_->stream_id());
                    close();
                    return;
                }
                mux_.send_data(session_->stream_id(), target_buf_.data(), n);
                do_read_from_target();
            });
    }

    void close()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!is_closed_)
        {
            is_closed_ = true;
            if (close_func_)
                close_func_(session_id_);
        }
    }

    asio::io_context &io_;
    SessionMux &mux_;
    std::shared_ptr<Session> session_;
    std::function<void(uint32_t stream_id)> close_func_;
    tcp::socket target_socket_;
    std::array<uint8_t, 8192> target_buf_{};
    std::deque<std::vector<uint8_t>> to_target_queue_;
    bool is_closed_ = false;

    uint32_t session_id_ = 0;

    std::mutex mutex_;
};

using asio::ip::udp;

class UdpClient : public std::enable_shared_from_this<UdpClient>
{
public:
    UdpClient(asio::io_context &io_context, SessionMux &mux, uint32_t session_id)
        : io_(io_context), socket_(io_context), mux_(mux), recv_buf_(65536), session_(mux.create_session(session_id))
    {
    }
    ~UdpClient()
    {
        std::cout << "UdpClient destroyed" << std::endl;
        socket_.close();
        if (session_)
        {
            mux_.remove_session(session_->stream_id());
        }
    }

    bool start()
    {
        asio::error_code ec;

        socket_.open(udp::v4(), ec);
        if (ec)
        {
            std::cout
                << "open failed:"
                << ec.message()
                << std::endl;
            mux_.send_udp_synack(session_->stream_id(), false);
            close();
            return false;
        }
        socket_.bind(udp::endpoint(udp::v4(), 0), ec);
        if (ec)
        {
            std::cout
                << "bind failed:"
                << ec.message()
                << std::endl;
            mux_.send_udp_synack(session_->stream_id(), false);
            close();
            return false;
        }
        std::cout << "UDP server listening on port " << socket_.local_endpoint().port() << std::endl;

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
                                     self->socket_.close(); });

        mux_.send_udp_synack(session_->stream_id(), true);

        return true;
    }

    void bind_close_func(std::function<void(uint32_t session_id)> close_func)
    {
        this->close_func_ = std::move(close_func);
    }

    void set_session_id(int32_t session_id)
    {
        this->session_id_ = session_id;
    }

private:
    // 发送到实际的地址
    void send(std::shared_ptr<std::vector<uint8_t>> data, const std::string &target_host, int target_port)
    {
        p2psocks::SendData send_data = {std::move(target_host), std::move(target_port), std::move(data)};
        send_queue_.push_back(std::move(send_data));
        if (!sending_)
        {
            do_send_next();
            sending_ = true;
        }
    }

    void do_send_next()
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
                    std::cerr << "resolve host failed " << ec.message() << "\n";
                    send_queue_.pop_front();
                    if (!send_queue_.empty())
                        do_send_next();
                    else
                        sending_ = false;
                    return;
                }

                socket_.async_send_to(
                    asio::buffer(*data),
                    results.begin()->endpoint(),
                    [self](std::error_code ec, std::size_t bytes_sent)
                    {
                        if (ec)
                        {
                            std::cout << "send error: "
                                      << ec.message()
                                      << std::endl;
                            self->close();
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
            });
    }
    void start_receive()
    {
        auto self = shared_from_this();
        /*timer_.expires_after(std::chrono::seconds(10));
        timer_.async_wait([self](std::error_code ec)
        {
            if (!ec)  // 定时器正常触发(没有被取消),说明超时了
            {
                std::cout << "UDP receive timeout, closing. stream_id=" << self->session_->stream_id() << std::endl;
                self->socket_.close();  // 这会让 async_receive_from 以 operation_aborted 返回
            }
        });*/

        socket_.async_receive_from(
            asio::buffer(recv_buf_), sender_endpoint_,
            [self](std::error_code ec, std::size_t bytes_recvd)
            {
                if (!ec)
                {
                    std::string host = self->sender_endpoint_.address().to_string();
                    uint16_t port = self->sender_endpoint_.port();

                    std::vector<uint8_t> reply(self->recv_buf_.begin(), self->recv_buf_.begin() + bytes_recvd);
                    self->mux_.send_udp(self->session_->stream_id(), host, port, reply);
                    self->start_receive();
                }
                else
                {
                    std::cout << "receive error: " << ec.message() << std::endl;
                    self->close();
                }
            });
    }

    void close()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!is_closed_)
        {
            is_closed_ = true;
        }
        else
        {
            return;
        }
        if (close_func_)
        {
            close_func_(session_id_);
        }
        socket_.close();
    }
    asio::io_context &io_;
    udp::socket socket_;
    udp::endpoint server_endpoint_;
    udp::endpoint sender_endpoint_;
    // asio::steady_timer timer_;
    SessionMux &mux_;
    std::vector<uint8_t> recv_buf_;
    std::shared_ptr<Session> session_;

    std::deque<p2psocks::SendData> send_queue_;
    std::function<void(uint32_t session_id)> close_func_;
    int32_t session_id_{0};

    bool sending_{false};
    std::mutex mutex_;
    bool is_closed_{false};
};
