#include "tcp_session.h"
#include <plog/Log.h>
#include <asio.hpp>
#include <sstream>
#include <deque>
#include <memory>

using asio::ip::tcp;
using namespace p2psocks;

Socks5Session::Socks5Session(asio::io_context &io, tcp::socket socket, SessionMux &mux, uint32_t session_id)
    : socket_(std::move(socket)), mux_(mux), io_(io), session_id_(session_id), http_response_parser_(http_parser_limits_), http_request_parser_(http_parser_limits_)
{
    PLOG_DEBUG << "Socks5Session created, session_id: " << session_id_;
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
                self->close();
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
                            self->close();
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
                                    self->close();
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
                std::ostream os(&request_buf_);
                os << buf_[0] << buf_[1];
                do_read_http_request_line();
            }
        });
}

void Socks5Session::do_read_http_request_line()
{
    auto self(shared_from_this());
    asio::async_read_until(
        socket_, request_buf_, "\r\n",
        [this, self](std::error_code ec, std::size_t length)
        {
            if (ec)
            {
                print_error("read https request line error");
                self->close();
                return;
            }

            std::istream is(&request_buf_);
            std::string line;
            std::getline(is, line);
            if (!line.empty() && line.back() == '\r')
                line.pop_back();

            std::istringstream iss(line);
            std::string method, uri, version;
            iss >> method >> uri >> version;

            if (method == "CONNECT")
            {
                auto pos = uri.find(':');
                if (pos == std::string::npos)
                {
                    PLOG_ERROR << "invalid CONNECT target: " << uri << " stream_id:" << self->session_->stream_id();
                    self->close();
                    return;
                }
                std::string http_host = uri.substr(0, pos);
                std::string http_port = uri.substr(pos + 1);

                target_host_ = http_host;
                target_port_ = std::stoi(http_port);

                consume_https_headers([this, self]()
                                      { request_remote_connect_for_https(); });
            }
            else
            {
                // 把已经读出的请求行，重新交给 HttpParser（因为它需要完整的
                // start-line 文本，而不是拆分好的 method/uri/version）

                std::string first_line = line + "\r\n";
                http_request_parser_.feed(first_line.data(), first_line.size());

                // 把 request_buf_ 里 async_read_until 已经多读进来、
                // 但还没被 getline 消费掉的剩余字节，也喂进去
                if (request_buf_.size() > 0)
                {
                    std::istream rest_is(&request_buf_);
                    std::string rest(
                        (std::istreambuf_iterator<char>(rest_is)),
                        std::istreambuf_iterator<char>());

                    if (!rest.empty())
                    {
                        http_request_parser_.feed(rest.data(), rest.size());
                    }
                }

                if (http_request_parser_.error())
                {
                    print_error("http parse error: " + http_request_parser_.error_message());
                    self->close();
                    return;
                }

                if (http_request_parser_.complete())
                {
                    // 一次性就凑齐了完整请求（常见于无 body 的 GET 请求）
                    on_http_request_complete();
                }

                // 还没收完，继续异步读取更多数据喂给 parser
                do_read_http_request_body();
            }
        });
}

void Socks5Session::do_read_http_request_body()
{
    auto self(shared_from_this());

    socket_.async_read_some(
        read_buf_.prepare(4096),
        [this, self](std::error_code ec, std::size_t length)
        {
            if (ec)
            {
                print_error("read http request body error");
                if (session_)
                {
                    mux_.send_http_fin(session_->stream_id());
                }
                self->close();
                return;
            }

            read_buf_.commit(length);

            auto bufs = read_buf_.data();
            std::string chunk(
                asio::buffers_begin(bufs),
                asio::buffers_begin(bufs) + length);

            read_buf_.consume(length);

            http_request_parser_.feed(chunk.data(), chunk.size());

            if (http_request_parser_.error())
            {
                print_error("http parse error: " + http_request_parser_.error_message());
                if (session_)
                {
                    mux_.send_http_fin(session_->stream_id());
                }
                self->close();
                return;
            }

            if (http_request_parser_.complete())
            {
                self->on_http_request_complete();
            }

            // 还没结束，继续读
            self->do_read_http_request_body();
        });
}

void Socks5Session::on_http_request_complete()
{
    auto msg = http_request_parser_.message();
    std::string next_line = http_request_parser_.take_remaining();
    http_request_parser_.reset();
    http_request_parser_.feed(next_line.data(), next_line.size());

    // 转发前的清理（hop-by-hop 头等），参考之前讨论的 prepare_for_forwarding
    // prepare_for_forwarding(msg, "1.1 my-proxy", socket_.remote_endpoint().address().to_string());

    pending_http_request_ = msg.serialize();
    if (!session_)
    {
        target_host_ = std::string(msg.get_header("Host"));

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
            close();
            return;
        }
        request_remote_connect_for_http();
    }
    else
    {
        mux_.send_http_data(session_->stream_id(), reinterpret_cast<const uint8_t *>(pending_http_request_.data()), pending_http_request_.size());
    }
}

void Socks5Session::request_remote_connect_for_http()
{
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
                              self->http_response_parser_.feed(reinterpret_cast<const char *>(d), n);
                              if (self->http_response_parser_.complete())
                              {
                                  auto &msg = self->http_response_parser_.message();
                                  prepare_for_forwarding(msg, "1.1 my-proxy", self->socket_.local_endpoint().address().to_string());
                                  bool writing = !self->write_queue_.empty();
                                  std::string s = msg.serialize();
                                  std::vector<uint8_t> v(s.size());
                                  std::memcpy(v.data(), s.data(), s.size());
                                  self->write_queue_.emplace_back(v);
                                  if (!writing)
                                      self->do_write_to_client();

                                  self->http_response_parser_.reset();
                                  std::string next_line = self->http_response_parser_.take_remaining();
                                  self->http_response_parser_.feed(next_line.data(), next_line.size());
                              } });
    session_->set_on_http_synack([weak_self](bool ok)
                                 {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                self->mux_.send_http_data(self->session_->stream_id(),
                                                     reinterpret_cast<const uint8_t *>(self->pending_http_request_.data()),
                                                     self->pending_http_request_.size()); });
    session_->set_on_http_close([weak_self]()
                                {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                std::error_code ec;
                                self->socket_.close(ec); });

    PLOG_INFO << "new http connect request " << target_host_ << ":"
              << target_port_ << " (stream_id=" << session_->stream_id()
              << ")\n";
    mux_.send_http_syn(session_->stream_id(), target_host_, target_port_);
}

void Socks5Session::do_connect_upstream_and_tunnel_for_https(bool ok)
{
    auto self(shared_from_this());

    static const char reply[] = "HTTP/1.1 200 Connection Established\r\n\r\n";
    static const char fail_reply[] = "HTTP/1.1 502 Bad Gateway\r\n\r\n";
    asio::async_write(
        socket_, asio::buffer(ok ? reply : fail_reply, ok ? sizeof(reply) - 1 : sizeof(fail_reply) - 1),
        [this, self](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                print_error("write 200 reply error");
                self->close();
                return;
            }
            do_read_from_client();
        });
}

void Socks5Session::request_remote_connect_for_https()
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
        bool writing = !self->write_queue_.empty();
        self->write_queue_.emplace_back(d, d + n);
        if (!writing) self->do_write_to_client(); });
    session_->set_on_synack([weak_self](bool ok)
                            {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                self->do_connect_upstream_and_tunnel_for_https(ok); });
    session_->set_on_close([weak_self]()
                           {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                std::error_code ec;
                                self->socket_.close(ec); });

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
                self->close();
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
                self->close();
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
        close();
        return;
    }
    if (!udp_session_)
    {
        udp_session_ = std::make_shared<UdpSession>(io_, mux_, session_, session_id_);
        if (!udp_session_->start())
        {
            send_socks_reply(0x01);
            close();
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
                self->close();
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
                self->close();
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
                        self->close();
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
                                    bool writing = !self->write_queue_.empty();
                                    self->write_queue_.emplace_back(d, d + n);
                                    if (!writing) self->do_write_to_client(); });
    session_->set_on_synack([weak_self](bool ok)
                            {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                self->send_socks_reply(ok ? 0x00 : 0x05); 
                                if (!ok)
                                {
                                    self->close();
                                } });
    session_->set_on_close([weak_self]()
                           {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                self->close(); });

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
                self->close();
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
                self->close();
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
                self->close();
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
                self->close();
                return;
            }
            self->do_read_from_client_for_udp();
        });
}

void Socks5Session::do_write_to_client()
{
    auto self(shared_from_this());
    asio::async_write(
        socket_, asio::buffer(write_queue_.front()),
        [self](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                self->print_error("write client error");
                self->close();
                return;
            }
            self->write_queue_.pop_front();
            if (!self->write_queue_.empty())
                self->do_write_to_client();
        });
}

void Socks5Session::print_error(const std::string &msg)
{
    PLOG_ERROR << "session " << session_id_ << " " << "stream_id=" << (session_.get() ? session_->stream_id() : 0) << " " << "host=" << target_host_ << " " << "port=" << target_port_ << " " << msg;
}

void Socks5Session::close()
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

    if (on_close_)
        on_close_(session_id_);
}