#include "tcp_session.h"
#include <plog/Log.h>
#include <asio.hpp>
#include <sstream>
#include <deque>
#include <memory>

using asio::ip::tcp;
using namespace p2psocks;

Socks5Session::Socks5Session(asio::io_context &io, tcp::socket socket, SessionMux &mux, uint32_t session_id)
    : socket_(std::move(socket)), mux_(mux), io_(io), session_id_(session_id)
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
                PLOG_DEBUG << "HTTP greeting received, session_id: " << session_id_;
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
                print_error("read http request line error");
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
                    PLOG_ERROR << "invalid CONNECT target: " << uri << " stream_id:"<<self->session_->stream_id();
                    self->close();
                    return;
                }
                std::string http_host = uri.substr(0, pos);
                std::string http_port = uri.substr(pos + 1);

                target_host_ = http_host;
                target_port_ = std::stoi(http_port);

                consume_http_headers([this, self]()
                                     { request_remote_connect_for_http(); });
            }
            else
            {
                print_error("plain http method not implemented: " + method);
                self->close();
            }
        });
}

void Socks5Session::do_connect_upstream_and_tunnel_for_http(bool ok)
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

void Socks5Session::request_remote_connect_for_http()
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
                                self->do_connect_upstream_and_tunnel_for_http(ok); });
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

    PLOG_INFO << "new http connect request " << target_host_ << ":"
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
                                    if (!writing) self->do_write_to_client(); 
                                });
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
                                }
                            });
    session_->set_on_close([weak_self]()
                           {
                                if (weak_self.expired())
                                {
                                    PLOG_WARNING << "Socks5Session expired";
                                    return;
                                }
                                auto self = weak_self.lock();
                                self->close();
                            });

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
                PLOG_ERROR << "read do_read_from_client error " << ec.message() <<"session_id=" << self->session_id_;
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
                PLOG_ERROR << "read do_read_from_client_for_udp error " << ec.message() <<"session_id=" << self->session_id_;
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
    PLOG_ERROR <<"session " << session_id_ << " " << "stream_id=" << (session_.get() ? session_->stream_id() : 0) << " " << "host=" << target_host_ << " " << "port=" << target_port_ << " " << msg;
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
    
    if(on_close_)
        on_close_(session_id_);
    
}