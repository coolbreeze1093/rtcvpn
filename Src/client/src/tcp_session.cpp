#include "tcp_session.h"
#include <plog/Log.h>
#include <asio.hpp>
#include <sstream>
#include <deque>
#include <memory>

using asio::ip::tcp;
using namespace p2psocks;

Socks5Session::Socks5Session(asio::io_context &io, tcp::socket socket, SessionMux &mux, uint32_t session_id)
    : socket_(std::move(socket)), mux_(mux), io_(io), session_id_(session_id), http_response_parser_(HttpParser::Type::Response), http_request_parser_(HttpParser::Type::Request)
{
    PLOG_DEBUG << "Socks5Session created, session_id: " << session_id_;
    http_request_parser_.set_on_headers_complete([this](HttpParser::Message &msg)
                                                 {
        if(msg.method == "CONNECT")
        {
            target_host_ = msg.target;
        }
        else
        {
            target_host_ = std::string(msg.get_header("Host"));
        }

        // Host 头可能带端口，比如 "example.com:8080"
        auto colon = target_host_.find(':');
        if (colon != std::string::npos)
        {
            target_port_ = std::stoi(target_host_.substr(colon + 1));
            target_host_ = target_host_.substr(0, colon);
        }
        else
        {
            target_port_ = 80;
        }

        if (target_host_.empty())
        {
            print_error("plain http request missing Host header");
            close_session();
            return;
        }

        if(msg.method == "CONNECT")
        {
            request_remote_connect_for_https();
        }
        else
        {
            request_remote_connect_for_http();
        } });

    http_request_parser_.set_on_error([this](int errno_code, std::string_view reason)
                                      {
                                                     PLOG_ERROR << "http parse error: " << errno_code << " reason: " << reason.data();
                                                     close_session(); });

    http_response_parser_.set_on_headers_complete([this](HttpParser::Message &msg)
                                                  {
                                                      //prepare_for_forwarding(msg, "1.1 my-proxy", socket_.local_endpoint().address().to_string());
                                                      std::string s = msg.serialize_headers_only();
                                                      //PLOG_DEBUG << "http headers: " << s;
                                                      write_to_client(reinterpret_cast<const uint8_t *>(s.data()), s.size()); });
    http_response_parser_.set_on_body([this](const char *data, std::size_t len)
                                      {
    if (http_response_parser_.message().chunked)
    {
        // 重新编码为 chunked 格式: <hex-length>\r\n<data>\r\n
        char hexbuf[24];
        int hlen = std::snprintf(hexbuf, sizeof(hexbuf), "%zx\r\n", len);

        std::vector<uint8_t> v;
        v.reserve(hlen + len + 2);

        v.insert(v.end(), hexbuf, hexbuf + hlen);   // 用 insert 代替 memcpy，size() 正确更新
        v.insert(v.end(), data, data + len);
        v.push_back('\r');
        v.push_back('\n');

        write_to_client(std::move(v));
    }
    else
    {
        write_to_client(reinterpret_cast<const uint8_t*>(data), len);
    } });

    http_response_parser_.set_on_error([this](int errno_code, std::string_view reason)
                                       {
                                                     PLOG_ERROR << "http parse error: " << errno_code << " reason: " << reason.data();
                                                     if (session_)
                                                    {
                                                        mux_.send_http_fin(session_->stream_id());
                                                    }
                                                     close_session(); });

    http_response_parser_.set_on_message_complete([this]()
                                                  {
                                                      if(http_response_parser_.message().chunked)
                                                    {
                                                        std::vector<uint8_t> v{'0', '\r', '\n', '\r', '\n'};
                                                        write_to_client(std::move(v)); 
                                                    } });
}

void Socks5Session::start() { do_read_greeting(); }

Socks5Session::~Socks5Session()
{
    PLOG_DEBUG << "Socks5Session destroyed, session_id: " << session_id_;
    if (udp_session_)
    {
        udp_session_->close();
    }
    if (session_)
    {
        mux_.remove_session(session_->stream_id());
    }
}

void Socks5Session::set_on_close(std::function<void(uint32_t)> on_close)
{
    this->on_close_ = std::move(on_close);
}

void Socks5Session::close()
{
    PLOG_DEBUG << "Socks5Session close, session_id: " << session_id_;
    if (socket_.is_open())
    {
        socket_.close();
    }
    else
    {
        PLOG_DEBUG << "Socks5Session close, session_id: " << session_id_ << " socket is closed";
    }
}

void Socks5Session::do_read_greeting()
{
    auto self(shared_from_this());
    asio::async_read(
        socket_, asio::buffer(buf_, 2),
        [this, self](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                print_error("read greeting error");
                self->close_session();
                return;
            }
            if (buf_[0] == 0x05)
            {
                PLOG_DEBUG << "SOCKS5 greeting received, session_id: " << session_id_;
                int nmethods = buf_[1];
                asio::async_read(
                    socket_, asio::buffer(buf_, nmethods),
                    [this, self](std::error_code ec, std::size_t)
                    {
                        if (ec)
                        {
                            print_error("read methods error");
                            self->close_session();
                            return;
                        }
                        static const uint8_t reply[2] = {0x05, 0x00};
                        asio::async_write(
                            socket_, asio::buffer(reply, 2),
                            [this, self](std::error_code ec, std::size_t)
                            {
                                if (ec)
                                {
                                    print_error("write reply error");
                                    self->close_session();
                                    return;
                                }
                                do_read_request();
                            });
                    });
                return;
            }
            else
            {
                PLOG_DEBUG << "HTTPs greeting received, session_id: " << session_id_;
                http_request_parser_.feed(reinterpret_cast<const char *>(buf_.data()), 2);
                do_read_http_request_line();
            }
        });
}

void Socks5Session::do_read_http_request_line()
{
    if (http_request_parser_.complete())
    {
        LOG_DEBUG << "do_read_http_request_line complete, session_id: " << session_id_;
        return;
    }
    auto self(shared_from_this());
    socket_.async_read_some(
        asio::buffer(request_buf_),
        [this, self](std::error_code ec, std::size_t length)
        {
            if (ec)
            {
                PLOG_ERROR << "read http request line error  " << ec.message();
                self->close_session();
                return;
            }
            http_request_parser_.feed(reinterpret_cast<const char *>(request_buf_.data()), length);
            do_read_http_request_line();
        });
}

void Socks5Session::request_remote_connect_for_http()
{
    state_ = State::HttpConnecting;
    session_ = mux_.create_session();
    std::weak_ptr<Socks5Session> weak_self = shared_from_this();
    session_->set_on_http_data([weak_self](const uint8_t *d, size_t n)
                               {
                              if (weak_self.expired())
                              {
                                  PLOG_WARNING << "Socks5Session expired";
                                  return;
                              }
                              auto self = weak_self.lock();
                              if(!self)
                              {
                                  PLOG_WARNING << "self expired";
                                  return;
                              }
                              
                              self->http_response_parser_.feed(reinterpret_cast<const char *>(d), n); });
    session_->set_on_http_synack([weak_self](bool ok)
                                 {
                                     if (weak_self.expired())
                                     {
                                         PLOG_WARNING << "Socks5Session expired";
                                         return;
                                     }
                                     auto self = weak_self.lock();
                                     if(!self)
                                     {
                                         PLOG_WARNING << "self expired";
                                         return;
                                     }
                                     self->state_ = State::HttpConnected;
                                     if(ok)
                                     {
                                         self->http_connected();

                                     }
                                     else
                                     {
                                         PLOG_ERROR << "http connect failed session_id=" << self->session_id_;
                                         std::error_code ec;
                                        self->socket_.close(ec);
                                        if(ec)
                                        {
                                            PLOG_ERROR << "close http socket error " << ec.message() << "session_id=" << self->session_id_;
                                        }
                                     } });
    session_->set_on_http_close([weak_self]()
                                {
                                    if (weak_self.expired())
                                    {
                                        PLOG_WARNING << "Socks5Session expired";
                                        return;
                                    }
                                    auto self = weak_self.lock();
                                    if (!self)
                                    {
                                        PLOG_WARNING << "self expired";
                                        return;
                                    }
                                    std::error_code ec;
                                    self->socket_.close(ec);
                                    if (ec)
                                    {
                                        PLOG_ERROR << "close http socket error " << ec.message() << "session_id=" << self->session_id_;
                                    }
                                    PLOG_INFO << "close http socket success session_id=" << self->session_id_; });

    PLOG_INFO << "new http connect request " << target_host_ << ":"
              << target_port_ << " (stream_id=" << session_->stream_id()
              << ") " << "session_id=" << session_id_;
    mux_.send_http_syn(session_->stream_id(), target_host_, target_port_);
}

void Socks5Session::http_connected()
{
    PLOG_DEBUG << "Socks5Session http_connected, session_id: " << session_id_ << ",stream_id: " << session_->stream_id();
    auto &msg = http_request_parser_.message();
    std::string s = msg.serialize();
    mux_.send_http_data(session_->stream_id(), reinterpret_cast<const uint8_t *>(s.data()), s.size());
    do_read_from_client_for_http();
}

void Socks5Session::do_read_from_client_for_http()
{
    auto self(shared_from_this());
    socket_.async_read_some(
        asio::buffer(client_buf_),
        [self](std::error_code ec, std::size_t n)
        {
            if (ec)
            {
                PLOG_ERROR << "read do_read_from_client_for_http error " << ec.message() << "session_id=" << self->session_id_;
                self->mux_.send_http_fin(self->session_->stream_id());
                self->close_session();
                return;
            }
            self->mux_.send_http_data(self->session_->stream_id(), self->client_buf_.data(), n);
            self->do_read_from_client_for_http();
        });
}

void Socks5Session::do_connect_upstream_and_tunnel_for_https(bool ok)
{
    auto self(shared_from_this());

    static const char reply[] = "HTTP/1.1 200 Connection Established\r\n\r\n";
    static const char fail_reply[] = "HTTP/1.1 502 Bad Gateway\r\n\r\n";
    asio::async_write(
        socket_, asio::buffer(ok ? reply : fail_reply, ok ? sizeof(reply) - 1 : sizeof(fail_reply) - 1),
        [this, self, ok](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                print_error("write 200 reply error");
                self->close_session();
                return;
            }
            if (ok)
            {
                do_read_from_client();
            }
        });
}

void Socks5Session::request_remote_connect_for_https()
{
    state_ = State::HttpsConnecting;
    session_ = mux_.create_session();
    std::weak_ptr<Socks5Session> weak_self = shared_from_this();
    session_->set_on_data([weak_self](const uint8_t *d, size_t n)
                          {
                            if (weak_self.expired())
                            {
                                PLOG_WARNING << "Socks5Session expired";
                                return;
                            }
                            auto self = weak_self.lock();
                            if(!self)
                            {
                                PLOG_WARNING << "self expired";
                                return;
                            }
                            self->write_to_client(d, n); });
    session_->set_on_synack([weak_self](bool ok)
                            {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                if(!self)
                                {
                                    PLOG_WARNING << "self expired";
                                    return;
                                }
                                self->state_ = State::HttpsConnected;
                                self->do_connect_upstream_and_tunnel_for_https(ok); 
                                if(!ok)
                                {
                                    PLOG_ERROR << "https connect failed session_id=" << self->session_id_;
                                    self->close_session();
                                    return;
                                }
                                PLOG_INFO << "https connect success session_id=" << self->session_id_; });
    session_->set_on_close([weak_self]()
                           {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                if(!self)
                                {
                                    PLOG_WARNING << "self expired";
                                    return;
                                }
                                std::error_code ec;
                                self->socket_.close(ec);
                                if (ec)
                                {
                                    PLOG_ERROR << "close https socket error " << ec.message() << "session_id=" << self->session_id_;
                                }
                                PLOG_INFO << "Socks5Session close session_id=" << self->session_id_; });

    PLOG_INFO << "new https connect request " << target_host_ << ":"
              << target_port_ << " (stream_id=" << session_->stream_id()
              << ")\n";
    mux_.send_syn(session_->stream_id(), target_host_, target_port_);
}

void Socks5Session::do_read_request()
{
    auto self(shared_from_this());
    asio::async_read(
        socket_, asio::buffer(buf_, 4),
        [self](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                self->print_error("read request error");
                self->close_session();
                return;
            }
            uint8_t cmd = self->buf_[1];
            uint8_t atyp = self->buf_[3];
            if (cmd == 0x01)
            {
                PLOG_DEBUG << "SOCKS5 connect request received, session_id: " << self->session_id_;
                if (atyp == 0x01)
                    self->read_ipv4();
                else if (atyp == 0x03)
                    self->read_domain();
                else
                    self->send_socks_reply(0x08);
                return;
            }
            else if (cmd == 0x03)
            {
                PLOG_DEBUG << "SOCKS5 UDP request received, session_id: " << self->session_id_;
                self->read_udp();
            }
            else
            {
                PLOG_WARNING << "invalid command: " << static_cast<int>(cmd);
                self->send_socks_reply(0x07);
                return;
            }
        });
}

void Socks5Session::read_udp()
{
    auto self(shared_from_this());
    asio::async_read(
        socket_, asio::buffer(buf_, 6),
        [self](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                self->print_error("read udp error");
                self->close_session();
                return;
            }
            char tmp[32];
            std::snprintf(tmp, sizeof(tmp), "%d.%d.%d.%d", self->buf_[0], self->buf_[1],
                          self->buf_[2], self->buf_[3]);
            self->target_host_ = tmp;
            self->target_port_ = (uint16_t(self->buf_[4]) << 8) | self->buf_[5];
            self->request_remote_connect_for_udp();
        });
}

void Socks5Session::request_remote_connect_for_udp()
{
    std::weak_ptr<Socks5Session> weak_self = shared_from_this();
    session_ = mux_.create_session();
    session_->set_on_udp_synack([weak_self](bool ok)
                                {
                                    if (weak_self.expired())
                                    {
                                        PLOG_WARNING << "Socks5Session expired";
                                        return;
                                    }
                                    auto self = weak_self.lock();
                                    self->do_connect_upstream_and_tunnel_for_udp(ok); });
    PLOG_INFO << "request_remote connect for udp, stream_id=" << session_->stream_id()
              << ", target_host=" << target_host_ << ", target_port=" << target_port_;
    mux_.send_udp_syn(session_->stream_id());
}

void Socks5Session::do_connect_upstream_and_tunnel_for_udp(bool ok)
{
    if (!ok)
    {
        send_socks_reply(0x07);
        close_session();
        return;
    }
    if (!udp_session_)
    {
        udp_session_ = std::make_shared<UdpSession>(io_, mux_, session_, session_id_);
        if (!udp_session_->start())
        {
            send_socks_reply(0x01);
            close_session();
            return;
        }
        do_read_from_client_for_udp();
    }

    send_udp_reply(udp_session_->get_local_ip(), udp_session_->getLocalPort());
}

void Socks5Session::read_ipv4()
{
    auto self(shared_from_this());
    asio::async_read(
        socket_, asio::buffer(buf_, 6),
        [self](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                self->print_error("read ipv4 error");
                self->close_session();
                return;
            }
            char tmp[32];
            std::snprintf(tmp, sizeof(tmp), "%d.%d.%d.%d", self->buf_[0], self->buf_[1],
                          self->buf_[2], self->buf_[3]);
            self->target_host_ = tmp;
            self->target_port_ = (uint16_t(self->buf_[4]) << 8) | self->buf_[5];
            self->request_remote_connect();
        });
}

void Socks5Session::read_domain()
{
    auto self(shared_from_this());
    asio::async_read(
        socket_, asio::buffer(buf_, 1),
        [self](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                self->print_error("read domain error");
                self->close_session();
                return;
            }
            int len = self->buf_[0];
            asio::async_read(
                self->socket_, asio::buffer(self->buf_, len + 2),
                [self, len](std::error_code ec, std::size_t)
                {
                    if (ec)
                    {
                        self->print_error("read domain error");
                        self->close_session();
                        return;
                    }
                    self->target_host_.assign(self->buf_.begin(), self->buf_.begin() + len);
                    self->target_port_ =
                        (uint16_t(self->buf_[len]) << 8) | self->buf_[len + 1];
                    self->request_remote_connect();
                });
        });
}

void Socks5Session::request_remote_connect()
{
    session_ = mux_.create_session();
    std::weak_ptr<Socks5Session> weak_self = shared_from_this();
    session_->set_on_data([weak_self](const uint8_t *d, size_t n)
                          {
                                    if (weak_self.expired())
                                    {
                                        PLOG_WARNING << "Socks5Session expired";
                                        return;
                                    }
                                    auto self = weak_self.lock();
                                    if (!self)
                                    {
                                        PLOG_WARNING << "Socks5Session close self expired";
                                        return;
                                    }
                                    self->write_to_client(d, n); });
    session_->set_on_synack([weak_self](bool ok)
                            {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                if (!self)
                                {
                                    PLOG_WARNING << "Socks5Session close self expired";
                                    return;
                                }
                                self->send_socks_reply(ok ? 0x00 : 0x05); 
                                if (!ok)
                                {
                                    asio::error_code ec;
                                   self->socket_.close(ec);
                                   if (ec)
                                   {
                                       PLOG_ERROR << "socket close error: " << ec.message()
                                                  << ", value=" << ec.value();
                                   }
                                   PLOG_INFO << "Socks5Session close session_id=" << self->session_id_;
                                   
                                } });
    session_->set_on_close([weak_self]()
                           {
                               if (weak_self.expired())
                               {
                                   PLOG_WARNING << "Socks5Session expired";
                                   return;
                               }
                               auto self = weak_self.lock();
                               if (self)
                               {
                                   asio::error_code ec;
                                   self->socket_.close(ec);
                                   if (ec)
                                   {
                                       PLOG_ERROR << "socket close error: " << ec.message()
                                                  << ", value=" << ec.value();
                                   }
                                   PLOG_INFO << "Socks5Session close session_id=" << self->session_id_;
                               }
                               else
                               {
                                   PLOG_WARNING << "Socks5Session close self expired";
                               } });

    PLOG_DEBUG << "new socks connect request " << target_host_ << ":"
               << target_port_ << " (stream_id=" << session_->stream_id()
               << ")\n";
    mux_.send_syn(session_->stream_id(), target_host_, target_port_);
}

void Socks5Session::send_socks_reply(uint8_t rep_code)
{
    auto self(shared_from_this());
    auto reply = std::make_shared<std::array<uint8_t, 10>>(
        std::array<uint8_t, 10>{0x05, rep_code, 0x00, 0x01, 0, 0, 0, 0, 0, 0});

    asio::async_write(
        socket_, asio::buffer(*reply),
        [self, rep_code, reply](std::error_code ec, std::size_t)
        {
            if (ec || rep_code != 0x00)
            {
                self->print_error("write reply error");
                self->close_session();
                return;
            }
            self->do_read_from_client();
        });
}

void Socks5Session::send_udp_reply(const std::string &host, int port)
{
    PLOG_INFO << "new udp connect request " << host << ":" << port << "\n";

    auto self(shared_from_this());
    auto reply = std::make_shared<std::vector<uint8_t>>();

    reply->push_back(0x05);
    reply->push_back(0x00);
    reply->push_back(0x00);
    reply->push_back(0x01);

    asio::ip::address_v4 addr =
        asio::ip::make_address_v4(host);

    auto bytes = addr.to_bytes();

    reply->insert(reply->end(),
                  bytes.begin(),
                  bytes.end());

    reply->push_back((port >> 8) & 0xff);
    reply->push_back(port & 0xff);

    asio::async_write(
        socket_, asio::buffer(*reply),
        [this, self, reply](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                print_error("write udp reply error");
                mux_.send_udp_fin(session_->stream_id());
                self->close_session();
                return;
            }
        });
}

void Socks5Session::do_read_from_client()
{
    auto self(shared_from_this());
    socket_.async_read_some(
        asio::buffer(client_buf_),
        [self](std::error_code ec, std::size_t n)
        {
            if (ec)
            {
                PLOG_ERROR << "read do_read_from_client error " << ec.message() << "session_id=" << self->session_id_;
                self->mux_.send_fin(self->session_->stream_id());
                self->close_session();
                return;
            }
            self->mux_.send_data(self->session_->stream_id(), self->client_buf_.data(), n);
            self->do_read_from_client();
        });
}

void Socks5Session::do_read_from_client_for_udp()
{
    auto self(shared_from_this());
    socket_.async_read_some(
        asio::buffer(client_buf_),
        [self](std::error_code ec, std::size_t n)
        {
            if (ec)
            {
                PLOG_ERROR << "read do_read_from_client_for_udp error " << ec.message() << "session_id=" << self->session_id_;
                self->mux_.send_udp_fin(self->session_->stream_id());
                self->close_session();
                return;
            }
            self->do_read_from_client_for_udp();
        });
}

void Socks5Session::write_to_client(const uint8_t *data, size_t size)
{
    write_queue_.emplace_back(data, data + size);
    if (!is_writing_)
    {
        is_writing_ = true;
        do_write_to_client();
    }
}

void Socks5Session::write_to_client(std::vector<uint8_t> v)
{
    write_queue_.emplace_back(std::move(v));
    if (!is_writing_)
    {
        is_writing_ = true;
        do_write_to_client();
    }
}

void Socks5Session::do_write_to_client()
{
    auto self(shared_from_this());

    constexpr size_t MAX_BUFFERS = 32;
    constexpr size_t MAX_BYTES = 64 * 1024;

    std::vector<asio::const_buffer> buffers;
    buffers.reserve(MAX_BUFFERS);

    size_t count = 0;
    size_t total_bytes = 0;

    for (auto &v : write_queue_)
    {
        buffers.emplace_back(asio::buffer(v));
        total_bytes += v.size();
        if (++count >= MAX_BUFFERS || total_bytes >= MAX_BYTES)
            break;
    }

    asio::async_write(
        socket_, buffers,
        [self, count](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                PLOG_ERROR << "write do_write_to_client error " << ec.message() << "session_id=" << self->session_id_;
                self->write_queue_.clear();
                self->close_session();
                return;
            }
            for (size_t i = 0; i < count; ++i)
            {
                self->write_queue_.pop_front();
            }

            if (self->ctrl_type_ == p2psocks::CtrlType::receive && self->write_queue_.size() > 1000)
            {
                self->ctrl_type_ = p2psocks::CtrlType::pause;
                self->mux_.send_data_ctrl(self->session_->stream_id(), p2psocks::CtrlType::pause);
            }
            else if (self->ctrl_type_ == p2psocks::CtrlType::pause && self->write_queue_.size() < 300)
            {
                self->ctrl_type_ = p2psocks::CtrlType::receive;
                self->mux_.send_data_ctrl(self->session_->stream_id(), p2psocks::CtrlType::receive);
            }

            if (!self->write_queue_.empty())
            {
                self->do_write_to_client();
            }
            else
            {
                self->is_writing_ = false;
            }
        });
}

void Socks5Session::print_error(const std::string &msg)
{
    PLOG_ERROR << "session " << session_id_ << " " << "stream_id=" << (session_.get() ? session_->stream_id() : 0) << " " << "host=" << target_host_ << " " << "port=" << target_port_ << " " << msg;
}

void Socks5Session::close_session()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (is_closed_)
            return;

        is_closed_ = true;
    }

    if (on_close_)
        on_close_(session_id_);
}