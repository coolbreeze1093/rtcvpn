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

class TcpSession : public std::enable_shared_from_this<TcpSession>
{
public:
    TcpSession(asio::io_context &io, SessionMux &mux,
                  uint32_t session_id)
        : io_(io), mux_(mux), target_socket_(io)
    {
        session_ = mux_.create_session(session_id);
        PLOG_DEBUG << "TcpSession created, stream_id=" << session_->stream_id();
    }

    ~TcpSession()
    {
        if (session_)
        {
            mux_.remove_session(session_->stream_id());
        }
        PLOG_DEBUG << "TcpSession close, stream_id=" << session_->stream_id();
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
                    PLOG_ERROR << "resolve host failed " << ec.message();
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
                            PLOG_ERROR << "connect target failed, " << ec.message();
                            mux_.send_synack(session_->stream_id(), false);
                            close();
                            return;
                        }
                        PLOG_DEBUG << "connect target success, stream_id="
                                  << session_->stream_id();
                        mux_.send_synack(session_->stream_id(), true);
                        setup_session_callbacks();
                        do_read_from_target();
                    });
            });
    }

private:
    void setup_session_callbacks()
    {
        std::weak_ptr<TcpSession> weak_self = shared_from_this();

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
                    PLOG_ERROR << "do_write_to_target error, stream_id=" << session_->stream_id();
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
                    PLOG_ERROR << "do_read_from_target error, stream_id=" << session_->stream_id();
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


