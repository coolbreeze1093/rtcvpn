#include "http_session.h"
#include <plog/Log.h>
#include <asio.hpp>

using asio::ip::tcp;
using namespace p2psocks;

HttpSession::HttpSession(asio::io_context &io, std::weak_ptr<SessionMux> weak_mux,
                         uint32_t stream_id)
    : io_(io), weak_mux_(weak_mux), target_socket_(io), stream_id_(stream_id),
      http_parse_request_(HttpParser::Type::Request),
      http_parse_response_(HttpParser::Type::Response)
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

    http_parse_request_.set_on_headers_complete([this](HttpParser::Message &msg)
                                                {
                                                      //prepare_for_forwarding(msg, "1.1 my-proxy", target_socket_.local_endpoint().address().to_string());
                                                      std::string s = msg.serialize_headers_only();
                                                      //PLOG_DEBUG << "http headers: " << s;
                                                      std::vector<uint8_t> v(s.size());
                                                      std::memcpy(v.data(), s.data(), s.size());
                                                      write_to_target(v); });
    http_parse_request_.set_on_body([this](const char *data, std::size_t len)
                                    {
                                  //PLOG_DEBUG << "http body: " << std::string(data, len);
                                  if (http_parse_request_.message().chunked)
    {
        // 重新编码为 chunked 格式: <hex-length>\r\n<data>\r\n
        std::ostringstream oss;
        oss << std::hex << len << "\r\n";
        std::string chunk_header = oss.str();

        std::vector<uint8_t> v;
        v.reserve(chunk_header.size() + len + 2);

        // chunk size 行
        v.insert(v.end(), chunk_header.begin(), chunk_header.end());
        // chunk data
        v.insert(v.end(), data, data + len);
        // 结尾 CRLF
        v.push_back('\r');
        v.push_back('\n');

        write_to_target(v);
    }
    else
    {
        std::vector<uint8_t> v(len);
        std::memcpy(v.data(), data, len);
        write_to_target(v);
    } });

    http_parse_request_.set_on_error([this](int errno_code, std::string_view reason)
                                     {
                                    PLOG_ERROR << "http parse error: " << errno_code << " reason: " << reason.data();
                                    close(); });

    http_parse_request_.set_on_message_complete([this]()
                                                {
                                                    if(http_parse_request_.message().chunked)
                                                    {
                                                        std::vector<uint8_t> v{'0', '\r', '\n', '\r', '\n'};
                                                        write_to_target(v); 
                                                    } });
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
                [this, self](std::error_code ec, const tcp::endpoint &endpoint)
                {
                    if (ec)
                    {
                        PLOG_ERROR << "connect target failed, stream_id=" << stream_id_ 
                        << ", host=" << host_ << ", port=" << port_<<",endpoint="<<endpoint.address().to_string()
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
                               << stream_id_ <<",host=" << host_ << ", address=" << endpoint.address().to_string()
                               << ", port=" << endpoint.port();
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

                              self->http_parse_request_.feed(reinterpret_cast<const char *>(d), n); });
    session_->set_on_http_close([weak_self]()
                                {
        if(weak_self.expired())
        {
            PLOG_ERROR << "weak_self is expired";
            return;
        }
        auto self = weak_self.lock();
        self->close(); });
    session_->set_on_data_ctrl([weak_self](CtrlType ctrl)
                                {
        if(weak_self.expired())
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
        switch (ctrl)
        {
        case CtrlType::receive:
            self->start_receive();
            break;
        case CtrlType::pause:
            self->pause_receive();
            break;
        }
        });
        
}

void HttpSession::write_to_target(const std::vector<uint8_t> &v)
{
    to_target_queue_.emplace_back(std::move(v));
    if (!is_sending_) 
    {
        is_sending_ = true;
        do_write_to_target(); 
    }
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
            {
                do_write_to_target();
            }
            else
            {
                is_sending_ = false;
            }
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
                PLOG_ERROR << "do_read_from_target error " << ec.message() << ", value=" << ec.value() << ", stream_id=" << stream_id_;
                close_func();
                if (weak_mux_.expired())
                {
                    PLOG_ERROR << "weak_mux is expired";
                    return;
                }
                else if (auto mux = weak_mux_.lock())
                {
                    mux->send_http_fin(stream_id_);
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
            if(is_receiving_)
            {
                do_read_from_target();
            }
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