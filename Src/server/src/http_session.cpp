#include "http_session.h"
#include <plog/Log.h>
#include <asio.hpp>

using asio::ip::tcp;
using namespace p2psocks;

HttpSession::HttpSession(asio::io_context &io, std::weak_ptr<SessionMux> weak_mux,
                         uint32_t stream_id)
    : io_(io), weak_mux_(weak_mux), target_socket_(io), stream_id_(stream_id),
      http_parse_request_(http_parser_limits_),
      http_parse_response_(http_parser_limits_)
{
    if (weak_mux_.expired())
    {
        PLOG_ERROR << "weak_mux is expired";
        return;
    }
    else
    {
        session_ = weak_mux_.lock()->create_session(stream_id_);
    }
    PLOG_DEBUG << "HttpSession created, stream_id=" << stream_id_;
}

HttpSession::~HttpSession()
{
    if (session_)
    {
        if (weak_mux_.expired())
        {
            PLOG_ERROR << "weak_mux is expired";
            return;
        }
        else
        {
            weak_mux_.lock()->remove_session(stream_id_);
        }
    }
    PLOG_DEBUG << "HttpSession close, stream_id=" << stream_id_;
}

void HttpSession::bind_close_func(std::function<void(uint32_t session_id)> func)
{
    close_func_ = func;
}

void HttpSession::connect_target(const std::string &host, uint16_t port)
{
    host_ = host;
    port_ = port;
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
                if (weak_mux_.expired())
                {
                    PLOG_ERROR << "weak_mux is expired";
                    return;
                }
                else
                {
                    weak_mux_.lock()->send_http_synack(stream_id_, false);
                }
                close_func();
                return;
            }
            asio::async_connect(
                target_socket_, results,
                [this, self](std::error_code ec, const tcp::endpoint &)
                {
                    if (ec)
                    {
                        PLOG_ERROR << "connect target failed, stream_id=" << stream_id_
                                   << ", " << ec.message();
                        if (weak_mux_.expired())
                        {
                            PLOG_ERROR << "weak_mux is expired";
                            return;
                        }
                        else
                        {
                            weak_mux_.lock()->send_http_synack(stream_id_, false);
                        }
                        close_func();
                        return;
                    }
                    PLOG_DEBUG << "connect target success, stream_id="
                               << stream_id_ << ", host=" << host_ << ", port=" << port_;
                    if (weak_mux_.expired())
                    {
                        PLOG_ERROR << "weak_mux is expired";
                        return;
                    }
                    else
                    {
                        weak_mux_.lock()->send_http_synack(stream_id_, true);
                    }
                    setup_session_callbacks();
                    do_read_from_target();
                });
        });
}

void HttpSession::close()
{
    if (target_socket_.is_open())
    {
        std::error_code ec;
        target_socket_.close(ec);
        if (ec)
        {
            PLOG_ERROR << "target close error: " << ec.message()
                       << ", value=" << ec.value();
        }
    }
}

void HttpSession::setup_session_callbacks()
{
    std::weak_ptr<HttpSession> weak_self = shared_from_this();

    session_->set_on_http_data([weak_self](const uint8_t *d, size_t n)
                               {
                              // 来自本地端(浏览器)的数据 -> 写给目标服务器
                              if (weak_self.expired())
                              {
                                  PLOG_ERROR << "weak_self is expired";
                                  return;
                              }
                              auto self = weak_self.lock();
                              if (!self)
                              {
                                  PLOG_ERROR << "self is expired";
                                  return;
                              }

                              self->http_parse_request_.feed(reinterpret_cast<const char *>(d), n);
                              if (self->http_parse_request_.complete())
                              {
                                  HttpParser::Message request = self->http_parse_request_.message();
                                  std::string next_request = self->http_parse_request_.take_remaining();
                                  self->http_parse_request_.reset();
                                  self->http_parse_request_.feed(next_request.c_str(), next_request.size());
                                  prepare_for_forwarding(request,"my_proxy",self->target_socket_.local_endpoint().address().to_string());
                                  std::string serialized_request = request.serialize();
                                  std::vector<uint8_t> serialized_request_vec = {serialized_request.begin(), serialized_request.end()};
                                  bool writing = !self->to_target_queue_.empty();
                                  self->to_target_queue_.emplace_back(serialized_request_vec);
                                  if (!writing)
                                      self->do_write_to_target();
                              } });
    session_->set_on_http_close([weak_self]()
                                {
        if(weak_self.expired())
        {
            PLOG_ERROR << "weak_self is expired";
            return;
        }
        auto self = weak_self.lock();
        self->close(); });
}

void HttpSession::do_write_to_target()
{
    auto self(shared_from_this());
    asio::async_write(
        target_socket_, asio::buffer(to_target_queue_.front()),
        [this, self](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                PLOG_ERROR << "do_write_to_target error, stream_id=" << stream_id_;
                close_func();
                return;
            }
            to_target_queue_.pop_front();
            if (!to_target_queue_.empty())
                do_write_to_target();
        });
}

void HttpSession::do_read_from_target()
{
    auto self(shared_from_this());
    target_socket_.async_read_some(
        asio::buffer(target_buf_),
        [this, self](std::error_code ec, std::size_t n)
        {
            if (ec)
            {
                PLOG_ERROR << "do_read_from_target error, stream_id=" << stream_id_;
                close_func();
                if (weak_mux_.expired())
                {
                    PLOG_ERROR << "weak_mux is expired";
                    return;
                }
                else
                {
                    weak_mux_.lock()->send_http_fin(stream_id_);
                }
                return;
            }
            if (weak_mux_.expired())
            {
                PLOG_ERROR << "weak_mux is expired";
                return;
            }
            else
            {
                weak_mux_.lock()->send_http_data(stream_id_, target_buf_.data(), n);
            }
            do_read_from_target();
        });
}

void HttpSession::close_func()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_closed_)
    {
        is_closed_ = true;
        if (close_func_)
            close_func_(stream_id_);
    }
}