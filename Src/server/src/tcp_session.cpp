#include "tcp_session.h"
#include <plog/Log.h>
#include <asio.hpp>

using asio::ip::tcp;
using namespace p2psocks;

TcpSession::TcpSession(asio::io_context &io, std::weak_ptr<SessionMux> weak_mux,
              uint32_t session_id)
    : io_(io), weak_mux_(weak_mux), target_socket_(io)
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
    PLOG_DEBUG << "TcpSession created, stream_id=" << session_->stream_id();
}

TcpSession::~TcpSession()
{
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
    PLOG_DEBUG << "TcpSession close, stream_id=" << session_->stream_id();
}

void TcpSession::bind_close_func(std::function<void(uint32_t session_id)> func)
{
    close_func_ = func;
}

void TcpSession::set_session_id(uint32_t session_id)
{
    session_id_ = session_id;
}

void TcpSession::connect_target(const std::string &host, uint16_t port)
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
                if(weak_mux_.expired())
                {
                    PLOG_ERROR << "weak_mux is expired";
                    return;
                }
                else
                {
                    weak_mux_.lock()->send_synack(session_->stream_id(), false);
                }
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
                        if(weak_mux_.expired())
                        {
                            PLOG_ERROR << "weak_mux is expired";
                            return;
                        }
                        else
                        {
                            weak_mux_.lock()->send_synack(session_->stream_id(), false);
                        }
                        close();
                        return;
                    }
                    PLOG_DEBUG << "connect target success, stream_id="
                              << session_->stream_id();
                    if(weak_mux_.expired())
                    {
                        PLOG_ERROR << "weak_mux is expired";
                        return;
                    }
                    else
                    {
                        weak_mux_.lock()->send_synack(session_->stream_id(), true);
                    }
                    setup_session_callbacks();
                    do_read_from_target();
                });
        });
}

void TcpSession::setup_session_callbacks()
{
    std::weak_ptr<TcpSession> weak_self = shared_from_this();

    session_->set_on_data([weak_self](const uint8_t *d, size_t n)
                          {
        // 来自本地端(浏览器)的数据 -> 写给目标服务器
        if(weak_self.expired())
        {
            PLOG_ERROR << "weak_self is expired";
            return;
        }
        auto self = weak_self.lock();

        bool writing = !self->to_target_queue_.empty();
        self->to_target_queue_.emplace_back(d, d + n);
        if (!writing) self->do_write_to_target(); });
    session_->set_on_close([weak_self]()
                           {
        if(weak_self.expired())
        {
            PLOG_ERROR << "weak_self is expired";
            return;
        }
        auto self = weak_self.lock();
        std::error_code ec;
        self->target_socket_.close(ec);
        self->close(); });
}

void TcpSession::do_write_to_target()
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

void TcpSession::do_read_from_target()
{
    auto self(shared_from_this());
    target_socket_.async_read_some(
        asio::buffer(target_buf_),
        [this, self](std::error_code ec, std::size_t n)
        {
            if (ec)
            {
                PLOG_ERROR << "do_read_from_target error, stream_id=" << session_->stream_id();
                close();
                if(weak_mux_.expired())
                {
                    PLOG_ERROR << "weak_mux is expired";
                    return;
                }
                else
                {
                    weak_mux_.lock()->send_fin(session_->stream_id());
                }
                return;
            }
            if(weak_mux_.expired())
            {
                PLOG_ERROR << "weak_mux is expired";
                return;
            }
            else
            {
                weak_mux_.lock()->send_data(session_->stream_id(), target_buf_.data(), n);
            }
            do_read_from_target();
        });
}

void TcpSession::close()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_closed_)
    {
        is_closed_ = true;
        if (close_func_)
            close_func_(session_id_);
    }
}