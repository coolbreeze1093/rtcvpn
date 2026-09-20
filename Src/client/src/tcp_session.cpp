#include "tcp_session.h"
#include <plog/Log.h>
#include <asio.hpp>
#include <sstream>
#include <deque>
#include <memory>

using asio::ip::tcp;
using namespace p2psocks;

static const uint8_t reply[] = "HTTP/1.1 200 Connection Established\r\n\r\n";
static const uint8_t fail_reply[] = "HTTP/1.1 502 Bad Gateway\r\n\r\n";

Socks5Session::Socks5Session(asio::io_context &io, tcp::socket socket, SessionMux &mux, uint32_t session_id)
    : tcp_socket_{std::make_shared<TcpSocket>(io, std::move(socket))}, mux_(mux), io_(io), session_id_(session_id), http_response_parser_(HttpParser::Type::Response), http_request_parser_(HttpParser::Type::Request)
{
    PLOG_DEBUG << "Socks5Session created, session_id: " << session_id_;
    http_request_parser_.set_on_headers_complete([this](HttpParser::Message &msg)
                                                 {
        PLOG_DEBUG << "http headers complete session_id: " << session_id_;
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
            PLOG_ERROR << "plain http request missing Host header";
            tcp_socket_->close();
            return;
        }

        if(msg.method == "CONNECT")
        {
            protocol_ = Protocol::HttpsConnect;
        }
        else
        {
            protocol_ = Protocol::HttpPlain;
        } 
        request_remote_connect(); });

    http_request_parser_.set_on_error([this](int errno_code, std::string_view reason)
                                      {
                                                     PLOG_ERROR << "http parse error: " << errno_code << " reason: " << reason.data();
                                                     tcp_socket_->close(); });

    http_response_parser_.set_on_headers_complete([this](HttpParser::Message &msg)
                                                  {
                                                      // prepare_for_forwarding(msg, "1.1 my-proxy", socket_.local_endpoint().address().to_string());
                                                      std::string s = msg.serialize_headers_only();
                                                      // PLOG_DEBUG << "http headers: " << s;
                                                      tcp_socket_->send(reinterpret_cast<const uint8_t *>(s.data()), s.size()); });
    http_response_parser_.set_on_body([this](const char *data, std::size_t len)
                                      {
    if (http_response_parser_.is_chunked())
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

        tcp_socket_->send(v.data(), v.size());
    }
    else
    {
        tcp_socket_->send(reinterpret_cast<const uint8_t*>(data), len);
    } });

    http_response_parser_.set_on_error([this](int errno_code, std::string_view reason)
                                       {
                                                     PLOG_ERROR << "http parse error: " << errno_code << " reason: " << reason.data();
                                                     tcp_socket_->close(); });

    http_response_parser_.set_on_message_complete([this]()
                                                  {
                                                      if(http_response_parser_.is_chunked())
                                                    {
                                                        static const std::vector<uint8_t> v{'0', '\r', '\n', '\r', '\n'};
                                                        tcp_socket_->send(v.data(), v.size()); 
                                                    } });

    
}

void Socks5Session::start() { 
    auto self = shared_from_this();
    tcp_socket_->setCloseCallback([this,self]()
                                  { close_session(); 
                                    tcp_socket_.reset();
                                });

    tcp_socket_->setDataCallback([this,self](const uint8_t *data, size_t len)
                                 { process_data(data, len); });

    tcp_socket_->setWriteQueueCallback([this,self](WriteQueueStatus status)
                                       {
                                            p2psocks::CtrlType ctrl = status == WriteQueueStatus::Danger
                                            ? p2psocks::CtrlType::pause : p2psocks::CtrlType::receive;

                                            mux_.send_data_ctrl(session_->stream_id(), ctrl, protocol_); });

    tcp_socket_->start(); }

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
    if (tcp_socket_)
    {
        tcp_socket_->close();
    }
}

void Socks5Session::process_data(const uint8_t *data, size_t len)
{
    if (phase_ == Phase::Connected)
    {
        switch (protocol_)
        {
        case p2psocks::Protocol::Unknown:
            break;
        case p2psocks::Protocol::Socks5Connect:
            mux_.send_data(session_->stream_id(), data, len, protocol_);
            break;
        case p2psocks::Protocol::HttpPlain:
            mux_.send_data(session_->stream_id(), data, len, protocol_);
            break;
        case p2psocks::Protocol::HttpsConnect:
            mux_.send_data(session_->stream_id(), data, len, protocol_);
            break;
        case p2psocks::Protocol::UdpAssociate:
            break;
        default:
        {
            PLOG_ERROR << "unknown protocol: " << static_cast<int>(protocol_) << ", session_id: " << session_id_;
        }
        break;
        }
    }
    else if (phase_ == Phase::Closed)
    {
        if (tcp_socket_)
        {
            tcp_socket_->close();
        }
        return;
    }
    else
    {
        consume_pending(data, len);
    }
}

void Socks5Session::consume_pending(const uint8_t *data, size_t len)
{
    // 循环消费：每处理完一个定长阶段就可能进入下一个定长阶段，直到数据不够或进入变长(http)阶段
    size_t consumed_len = 0;
    while (phase_ != Phase::Closed &&
           phase_ != Phase::WaitHttpRequestHeaders &&
           phase_ != Phase::Connecting &&
           phase_ != Phase::Connected)
    {
        if(consumed_len >= len)
        {
            break;
        }
        switch (phase_)
        {
        case Phase::WaitGreetingVersion:
            consumed_len += handle_greeting_version(data, len);
            break;
        case Phase::WaitSocksMethods:
            consumed_len += handle_socks_methods(data, len);
            break;
        case Phase::WaitSocksTCP:
        case Phase::WaitSocksUDP:
            consumed_len += handle_socks_tcp_udp(data, len);
            break;
        case Phase::WaitSocksIpv4:
            consumed_len += handle_socks_ipv4(data, len);
            break;
        case Phase::WaitSocksDomain:
            consumed_len += handle_socks_domain(data, len);
            break;
        default:
            return;
        }
        if (phase_ == Phase::Closed)
        {
            tcp_socket_->close();
            return;
        }
    }

    // 如果转入了 HTTP 请求头阶段，把 pending_ 里剩余（本次判定为明文http的前2字节等）残留字节喂给 parser
    if (phase_ == Phase::WaitHttpRequestHeaders)
    {
        http_request_parser_.feed(reinterpret_cast<const char *>(data), len);
        pending_buf_.push_back({data, data + len});
    }
}

size_t Socks5Session::handle_greeting_version(const uint8_t *data, size_t len)
{
    uint8_t v0 = data[0];
    if (v0 == 0x05)
    {
        // int nmethods = data[1];
        phase_ = Phase::WaitSocksMethods;
        static const uint8_t reply[2] = {0x05, 0x00};
        tcp_socket_->send(reply, sizeof(reply));
    }
    else
    {
        // 明文 HTTP：这2个字节本身就是请求的一部分，留给 http parser
        phase_ = Phase::WaitHttpRequestHeaders;
    }

    return 2;
}

size_t Socks5Session::handle_socks_methods(const uint8_t *data, size_t len)
{
    uint8_t cmd = data[1];

    if (cmd == 0x01)
    {
        PLOG_DEBUG << "SOCKS5 connect request received, session_id: " << session_id_;
        phase_ = Phase::WaitSocksTCP;
        protocol_ = Protocol::Socks5Connect;
    }
    else if (cmd == 0x03)
    {
        PLOG_DEBUG << "SOCKS5 UDP request received, session_id: " << session_id_;
        phase_ = Phase::WaitSocksUDP;
        protocol_ = Protocol::UdpAssociate;
    }
    else
    {
        PLOG_WARNING << "invalid command: " << static_cast<int>(cmd);
        phase_ = Phase::Closed;
        send_socks_reply(0x07);
        return 1;
    }
    return 2;
}

size_t Socks5Session::handle_socks_tcp_udp(const uint8_t *data, size_t len)
{
    PLOG_DEBUG << "SOCKS5 connect request IPv4 address received, session_id: " << session_id_;
    uint8_t atyp = data[3];
    if (atyp == 0x01)
        phase_ = Phase::WaitSocksIpv4;
    else if (atyp == 0x03)
        phase_ = Phase::WaitSocksDomain;
    else
    {
        send_socks_reply(0x08);
        phase_ = Phase::Closed;
    }
    return 2;
}

size_t Socks5Session::handle_socks_ipv4(const uint8_t *data, size_t len)
{
    PLOG_DEBUG << "SOCKS5 connect request IPv4 address received, session_id: " << session_id_;
    char tmp[32];
    std::snprintf(tmp, sizeof(tmp), "%d.%d.%d.%d", data[4], data[5],
                  data[6], data[7]);
    target_host_ = tmp;
    target_port_ = (uint16_t(data[8]) << 8) | data[9];
    PLOG_DEBUG << "SOCKS5 connect request IPv4 address: " << target_host_ << ", port: " << target_port_ << ", session_id: " << session_id_;
    request_remote_connect();
    return 6;
}

size_t Socks5Session::handle_socks_domain(const uint8_t *data, size_t len)
{
    PLOG_DEBUG << "SOCKS5 connect request domain name length received, session_id: " << session_id_;
    int domain_len = data[4];
    target_host_.assign(data + 5, data + 5 + domain_len);
    target_port_ =
        (uint16_t(data[domain_len + 5]) << 8) | data[domain_len + 6];
    PLOG_DEBUG << "SOCKS5 connect request domain name: " << target_host_ << ", port: " << target_port_ << ", session_id: " << session_id_;
    request_remote_connect();
    return 3 + domain_len;
}

void Socks5Session::http_connected(bool ok)
{
    PLOG_DEBUG << "Socks5Session http_connected, session_id: " << session_id_ << ",stream_id: " << session_->stream_id();
    if (ok)
    {
        for (auto &buf : pending_buf_)
        {
            mux_.send_data(session_->stream_id(), reinterpret_cast<const uint8_t *>(buf.data()), buf.size(), protocol_);
        }
        pending_buf_.clear();
    }
    else
    {
        tcp_socket_->send(&fail_reply[0], sizeof(fail_reply) - 1);
    }
}

void Socks5Session::https_connected(bool ok)
{
    auto self(shared_from_this());
    if (ok)
    {
        tcp_socket_->send(&reply[0], sizeof(reply) - 1);
    }
    else
    {
        tcp_socket_->send(&fail_reply[0], sizeof(fail_reply) - 1);
    }
}

void Socks5Session::udp_connected(bool ok)
{
    if (!ok)
    {
        send_socks_reply(0x07);
        return;
    }
    if (!udp_session_)
    {
        udp_session_ = std::make_shared<UdpSession>(io_, mux_, session_id_);
        if (!udp_session_->start())
        {
            send_socks_reply(0x01);
            return;
        }
    }

    send_udp_reply(udp_session_->get_local_ip(), udp_session_->getLocalPort());
}

void Socks5Session::request_remote_connect()
{
    session_ = mux_.create_session();
    std::weak_ptr<Socks5Session> weak_self = shared_from_this();
    session_->set_on_data([weak_self](const uint8_t *d, size_t n, Protocol protocol)
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
                                    self->p2p_data(d, n, protocol); });
    session_->set_on_synack([weak_self](bool ok, Protocol protocol)
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
                                self->p2p_synack(ok, protocol); });
    session_->set_on_close([weak_self](Protocol protocol)
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
                               self->p2p_close(protocol); });

    PLOG_DEBUG << "new socks connect request " << target_host_ << ":"
               << target_port_ << " (stream_id=" << session_->stream_id()
               << ")\n";
    mux_.send_syn(session_->stream_id(), target_host_, target_port_, protocol_);
    phase_ = Phase::Connecting;
}

void Socks5Session::p2p_data(const uint8_t *d, size_t n, Protocol protocol)
{
    switch (protocol)
    {
    case Protocol::Unknown:
        break;
    case Protocol::UdpAssociate:
        udp_session_->revP2pData(d, n);
        break;
    case Protocol::HttpPlain:
        http_response_parser_.feed(reinterpret_cast<const char *>(d), n);
        break;
    case Protocol::Socks5Connect:
    case Protocol::HttpsConnect:
        tcp_socket_->send(d, n);
        break;
    default:
        break;
    }
}

void Socks5Session::p2p_synack(bool ok, Protocol protocol)
{
    switch (protocol)
    {
    case Protocol::Unknown:
        break;
    case Protocol::UdpAssociate:
        udp_connected(ok);
        break;
    case Protocol::HttpPlain:
        http_connected(ok);
        break;
    case Protocol::Socks5Connect:
        send_socks_reply(ok ? 0x00 : 0x05);
        break;
    case Protocol::HttpsConnect:
        https_connected(ok);
        break;
    default:
        break;
    }

    if (!ok)
    {
        phase_ = Phase::Closed;
        tcp_socket_->close();
        PLOG_INFO << "Socks5Session close session_id=" << session_id_;
    }
    else
    {
        phase_ = Phase::Connected;
    }
}

void Socks5Session::p2p_close(Protocol protocol)
{
    /* switch (protocol)
    {
    case Protocol::Unknown:
        break;
    case Protocol::UdpAssociate:
        break;
    case Protocol::HttpPlain:
        break;
    case Protocol::Socks5Connect:
        break;
    case Protocol::HttpsConnect:
        break;
    default:
        break;
    }
 */
    is_p2p_closed_ = true;
    if (tcp_socket_)
    {
        tcp_socket_->close();
    }

    PLOG_INFO << "Socks5Session close session_id=" << session_id_;
}

void Socks5Session::p2p_data_ctrl(p2psocks::CtrlType ctrl, Protocol protocol)
{
    switch (protocol)
    {
    case Protocol::Unknown:
        break;
    case Protocol::UdpAssociate:
        udp_session_->p2p_data_ctrl(ctrl);
        break;
    case Protocol::HttpPlain:
    case Protocol::Socks5Connect:
    case Protocol::HttpsConnect:
        switch (ctrl)
        {
        case CtrlType::pause:
            tcp_socket_->stopReading();
            break;
        case CtrlType::receive:
            tcp_socket_->startReading();
            break;
        default:
            break;
        }
        break;
    default:
        break;
    }
}

void Socks5Session::send_socks_reply(uint8_t rep_code)
{
    auto self(shared_from_this());
    auto reply = std::make_shared<std::array<uint8_t, 10>>(
        std::array<uint8_t, 10>{0x05, rep_code, 0x00, 0x01, 0, 0, 0, 0, 0, 0});

    tcp_socket_->send(reply->data(), reply->size());
}

void Socks5Session::send_udp_reply(const std::string &host, int port)
{
    PLOG_INFO << "new udp connect request " << host << ":" << port << "\n";

    auto self(shared_from_this());
    std::vector<uint8_t> reply;

    reply.push_back(0x05);
    reply.push_back(0x00);
    reply.push_back(0x00);
    reply.push_back(0x01);

    asio::ip::address_v4 addr =
        asio::ip::make_address_v4(host);

    auto bytes = addr.to_bytes();

    reply.insert(reply.end(),
                 bytes.begin(),
                 bytes.end());

    reply.push_back((port >> 8) & 0xff);
    reply.push_back(port & 0xff);

    tcp_socket_->send(reply.data(), reply.size());
}

void Socks5Session::close_session()
{
    PLOG_DEBUG << "Socks5Session close session_id=" << session_id_;
    if (!is_p2p_closed_ && session_)
    {
        mux_.send_fin(session_->stream_id(), protocol_);
        PLOG_DEBUG << "Socks5Session send_fin session_id=" << session_id_ << " stream_id=" << session_->stream_id();
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (is_closed_)
            return;

        is_closed_ = true;
    }

    if (on_close_)
        on_close_(session_id_);
}