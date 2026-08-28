// local.cpp —— 本地端 (Ingress)
// 职责：接收浏览器 SOCKS5 连接 -> 解析目标host:port -> 通过 SessionMux
//       (最终调用你的P2P模块 send()) 把请求发给远端 -> 远端连接成功后
//       双向转发数据。
//
// !!! 需要你接入的地方，我都用 "TODO(P2P)" 标出了 !!!
//
// 编译:
//   g++ -std=c++17 -O2 -DASIO_STANDALONE local.cpp -lpthread -o local
#pragma once
#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>

#include "session_mux.h"

using asio::ip::tcp;
using namespace p2psocks;
using asio::ip::udp;

class UdpServer : public std::enable_shared_from_this<UdpServer>
{
public:
    UdpServer(asio::io_context &io_context, SessionMux &mux)
        : socket_(io_context), recv_buf_(65535), io_(io_context), mux_(mux)
    {
        session_ = mux_.create_session();
        session_->set_on_udp([this](const std::string &local_host, uint16_t local_port,
                                    const std::string &remote_host, uint16_t remote_port,
                                    const std::vector<uint8_t> &data)
                             {
        auto reply = std::make_shared<std::vector<uint8_t>>();

        reply->push_back(0x00); // VER
        reply->push_back(0x00); // REP
        reply->push_back(0x00); // RSV
        reply->push_back(0x01); // ATYP IPv4

        // 简单示例：IPv4
        asio::ip::address_v4 addr =
            asio::ip::make_address_v4(local_host);

        auto bytes = addr.to_bytes();

        reply->insert(reply->end(),
                      bytes.begin(),
                      bytes.end());

        reply->push_back((local_port >> 8) & 0xff);
        reply->push_back(local_port & 0xff);

        do_send_to_client(reply, remote_host, remote_port); });
    }

    ~UdpServer()
    {
        std::cout << "UdpServer destroyed" << std::endl;
        mux_.remove_session(session_->stream_id());
    }

    void close()
    {
        socket_.close();
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
            return false;
        }
        socket_.bind(udp::endpoint(udp::v4(), 0), ec);
        if (ec)
        {
            std::cout
                << "bind failed:"
                << ec.message()
                << std::endl;
            return false;
        }
        std::cout << "UDP server listening on port " << socket_.local_endpoint().port() << std::endl;

        start_receive();
        return true;
    }

    int getLocalPort()
    {
        return socket_.local_endpoint().port();
    }

    std::string get_local_ip()
    {
        asio::ip::udp::socket sock(io_);

        sock.open(asio::ip::udp::v4());

        sock.connect(
            asio::ip::udp::endpoint(
                asio::ip::make_address("114.114.114.114"),
                53));

        return sock.local_endpoint()
            .address()
            .to_string();
    }

private:
    void start_receive()
    {
        auto self(shared_from_this());
        socket_.async_receive_from(
            asio::buffer(recv_buf_), remote_endpoint_,
            [this, self](std::error_code ec, std::size_t bytes_recvd)
            {
                if (!ec && bytes_recvd > 0)
                {
                    if (recv_buf_[3] == 0x01)
                    {
                        // 处理 IPv4 连接
                        send_ipv4(bytes_recvd, remote_endpoint_.address().to_string(), remote_endpoint_.port());
                    }
                    else if (recv_buf_[3] == 0x03)
                    {
                        send_domain(bytes_recvd, remote_endpoint_.address().to_string(), remote_endpoint_.port());
                    }
                    else
                    {
                    }
                }
                else if (ec)
                {
                    std::cout << "receive error: " << ec.message() << std::endl;
                    return;
                }

                // 继续监听下一个包
                start_receive();
            });
    }

    void send_ipv4(std::size_t bytes_recvd, const std::string& local_host, uint16_t local_port)
    {
        char tmp[32];
        std::snprintf(tmp, sizeof(tmp), "%d.%d.%d.%d", recv_buf_[4], recv_buf_[5],
                      recv_buf_[6], recv_buf_[7]);
        std::string target_host = tmp;
        int target_port = (uint16_t(recv_buf_[8]) << 8) | recv_buf_[9];
        std::vector<uint8_t> data(recv_buf_.begin() + 10, recv_buf_.begin() + bytes_recvd);

        mux_.send_udp(session_->stream_id(), local_host, local_port, target_host, target_port, data);
    }

    void send_domain(std::size_t bytes_recvd, const std::string& local_host, uint16_t local_port)
    {
        int len = recv_buf_[4];
        std::string target_host(recv_buf_.begin() + 5, recv_buf_.begin() + 5 + len);
        int target_port = (uint16_t(recv_buf_[6 + len]) << 8) | recv_buf_[7 + len];
        std::vector<uint8_t> data(recv_buf_.begin() + 8 + len, recv_buf_.begin() + bytes_recvd);

        mux_.send_udp(session_->stream_id(), local_host, local_port, target_host, target_port, data);
    }

    void do_send_to_client(std::shared_ptr<std::vector<uint8_t>> data, const std::string& target_host, int target_port)
    {
        auto target_endpoint = asio::ip::udp::endpoint(
            asio::ip::make_address(target_host),
            target_port);

        socket_.async_send_to(
            asio::buffer(*data),
            target_endpoint,
            [this](std::error_code ec, std::size_t bytes_sent)
            {
                if (ec)
                {
                    std::cout << "send error: "
                              << ec.message()
                              << std::endl;
                }
            });
    }

    udp::socket socket_;
    udp::endpoint remote_endpoint_;
    std::vector<uint8_t> recv_buf_;
    asio::io_context &io_;
    std::shared_ptr<Session> session_;
    SessionMux &mux_;
};

class ClientSession : public std::enable_shared_from_this<ClientSession>
{
public:
    ClientSession(asio::io_context &io, tcp::socket socket, SessionMux &mux)
        : socket_(std::move(socket)), mux_(mux), io_(io) {}

    void start() { do_read_greeting(); }

    ~ClientSession()
    {
        std::cout << "ClientSession destroyed" << std::endl;
        if (udp_session_)
        {
            udp_session_->close();
        }
    }

private:
    asio::streambuf request_buf_;

    void do_read_greeting()
    {
        auto self(shared_from_this());
        // 先只 peek 1 个字节判断协议类型，而不是直接 read 2 字节
        asio::async_read(
            socket_, asio::buffer(buf_, 2),
            [this, self](std::error_code ec, std::size_t)
            {
                if (ec)
                {
                    print_error("read greeting error");
                    return;
                }
                // -------- 标准 SOCKS5 握手 --------
                if (buf_[0] == 0x05)
                {
                    int nmethods = buf_[1];
                    asio::async_read(
                        socket_, asio::buffer(buf_, nmethods),
                        [this, self](std::error_code ec, std::size_t)
                        {
                            if (ec)
                            {
                                print_error("read methods error");
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
                                        return;
                                    }
                                    do_read_request();
                                });
                        });
                    return;
                }
                else
                {
                    // 不是 SOCKS5，走 HTTP 代理逻辑
                    // 已经吃掉了第一个字节，先塞回 streambuf
                    std::ostream os(&request_buf_);
                    os << buf_[0] << buf_[1];
                    do_read_http_request_line();
                }
            });
    }

    void do_read_http_request_line()
    {
        auto self(shared_from_this());
        asio::async_read_until(
            socket_, request_buf_, "\r\n",
            [this, self](std::error_code ec, std::size_t length)
            {
                if (ec)
                {
                    print_error("read http request line error");
                    return;
                }

                std::istream is(&request_buf_);
                std::string line;
                std::getline(is, line);
                if (!line.empty() && line.back() == '\r')
                    line.pop_back();

                // 解析请求行: METHOD URI HTTP/x.x
                std::istringstream iss(line);
                std::string method, uri, version;
                iss >> method >> uri >> version;

                if (method == "CONNECT")
                {
                    // uri 形如 host:port
                    auto pos = uri.find(':');
                    if (pos == std::string::npos)
                    {
                        std::cout << "invalid CONNECT target: " << uri << std::endl;
                        return;
                    }
                    std::string http_host = uri.substr(0, pos);
                    std::string http_port = uri.substr(pos + 1);

                    target_host_ = http_host;
                    target_port_ = std::stoi(http_port);

                    // 继续把剩余头部读完（直到空行），丢弃即可
                    consume_http_headers([this, self]()
                                         { request_remote_connect_for_http(); });
                }
                else
                {
                    // 普通 HTTP 请求（GET/POST等），需要从 URI 或 Host 头解析目标
                    // 这里简单示例：仅处理 CONNECT，其他方法先给出提示
                    print_error("plain http method not implemented: " + method);
                    // 也可以按需实现明文 HTTP 转发
                }
            });
    }

    // 读掉剩余 header，直到空行 "\r\n"
    template <typename Handler>
    void consume_http_headers(Handler handler)
    {
        auto self(shared_from_this());
        asio::async_read_until(
            socket_, request_buf_, "\r\n\r\n",
            [this, self, handler](std::error_code ec, std::size_t)
            {
                if (ec)
                {
                    print_error("read http headers error");
                    return;
                }
                // request_buf_ 中此时已包含全部头部，直接丢弃（consume）
                request_buf_.consume(request_buf_.size());
                handler();
            });
    }

    void do_connect_upstream_and_tunnel(bool ok)
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
                    return;
                }
                do_read_from_client();
            });
    }

    void request_remote_connect_for_http()
    {
        session_ = mux_.create_session();
        auto self(shared_from_this());
        session_->set_on_data([this, self](const uint8_t *d, size_t n)
                              {
            bool writing = !write_queue_.empty();
            write_queue_.emplace_back(d, d + n);
            if (!writing) do_write_to_client(); });
        session_->set_on_synack([this, self](bool ok)
                                { do_connect_upstream_and_tunnel(ok); });
        session_->set_on_close([this, self]()
                               {
            std::error_code ec;
            socket_.close(ec); });

        std::cout << "new http connect request " << target_host_ << ":"
                  << target_port_ << " (stream_id=" << session_->stream_id()
                  << ")\n";
        mux_.send_syn(session_->stream_id(), target_host_, target_port_);
    }

    void do_read_request()
    {
        auto self(shared_from_this());
        asio::async_read(
            socket_, asio::buffer(buf_, 4),
            [this, self](std::error_code ec, std::size_t)
            {
                if (ec)
                {
                    print_error("read request error");
                    return;
                }
                uint8_t cmd = buf_[1];
                uint8_t atyp = buf_[3];
                if (cmd == 0x01)
                {
                    if (atyp == 0x01)
                        read_ipv4();
                    else if (atyp == 0x03)
                        read_domain();
                    else
                        send_socks_reply(0x08);
                    return;
                }
                else if (cmd == 0x03)
                {
                    read_udp();
                }
                else
                {
                    // 只支持 CONNECT
                    std::cout << "invalid command: " << static_cast<int>(cmd) << std::endl;
                    send_socks_reply(0x07);
                    return;
                }
            });
    }

    void read_udp()
    {
        auto self(shared_from_this());
        asio::async_read(
            socket_, asio::buffer(buf_, 6),
            [this, self](std::error_code ec, std::size_t)
            {
                if (ec)
                {
                    print_error("read ipv4 error");
                    return;
                }
                char tmp[32];
                std::snprintf(tmp, sizeof(tmp), "%d.%d.%d.%d", buf_[0], buf_[1],
                              buf_[2], buf_[3]);
                target_host_ = tmp;
                target_port_ = (uint16_t(buf_[4]) << 8) | buf_[5];
                request_remote_connect_for_udp();
            });
    }

    void request_remote_connect_for_udp()
    {
        if (!udp_session_)
        {
            udp_session_ = std::make_shared<UdpServer>(io_, mux_);
            if (!udp_session_->start())
            {
                send_socks_reply(0x01);
                return;
            }
            do_read_from_client_for_udp();
        }

        send_udp_reply(udp_session_->get_local_ip(), udp_session_->getLocalPort());
    }

    void read_ipv4()
    {
        auto self(shared_from_this());
        asio::async_read(
            socket_, asio::buffer(buf_, 6),
            [this, self](std::error_code ec, std::size_t)
            {
                if (ec)
                {
                    print_error("read ipv4 error");
                    return;
                }
                char tmp[32];
                std::snprintf(tmp, sizeof(tmp), "%d.%d.%d.%d", buf_[0], buf_[1],
                              buf_[2], buf_[3]);
                target_host_ = tmp;
                target_port_ = (uint16_t(buf_[4]) << 8) | buf_[5];
                request_remote_connect();
            });
    }

    void read_domain()
    {
        auto self(shared_from_this());
        asio::async_read(
            socket_, asio::buffer(buf_, 1),
            [this, self](std::error_code ec, std::size_t)
            {
                if (ec)
                {
                    print_error("read domain error");
                    return;
                }
                int len = buf_[0];
                asio::async_read(
                    socket_, asio::buffer(buf_, len + 2),
                    [this, self, len](std::error_code ec, std::size_t)
                    {
                        if (ec)
                        {
                            print_error("read domain error");
                            return;
                        }
                        target_host_.assign(buf_.begin(), buf_.begin() + len);
                        target_port_ =
                            (uint16_t(buf_[len]) << 8) | buf_[len + 1];
                        request_remote_connect();
                    });
            });
    }

    // -------- 通过 P2P 隧道请求远端建立到目标的连接 --------
    void request_remote_connect()
    {
        session_ = mux_.create_session();
        auto self(shared_from_this());
        session_->set_on_data([this, self](const uint8_t *d, size_t n)
                              {
            bool writing = !write_queue_.empty();
            write_queue_.emplace_back(d, d + n);
            if (!writing) do_write_to_client(); });
        session_->set_on_synack([this, self](bool ok)
                                { send_socks_reply(ok ? 0x00 : 0x05); });
        session_->set_on_close([this, self]()
                               {
            std::error_code ec;
            socket_.close(ec); });

        std::cout << "new socks connect request " << target_host_ << ":"
                  << target_port_ << " (stream_id=" << session_->stream_id()
                  << ")\n";
        mux_.send_syn(session_->stream_id(), target_host_, target_port_);
    }

    void send_socks_reply(uint8_t rep_code)
    {
        auto self(shared_from_this());
        std::array<uint8_t, 10> reply = {0x05, rep_code, 0x00, 0x01,
                                         0, 0, 0, 0,
                                         0, 0};
        asio::async_write(
            socket_, asio::buffer(reply),
            [this, self, rep_code](std::error_code ec, std::size_t)
            {
                if (ec || rep_code != 0x00)
                {
                    print_error("write reply error");
                    return;
                }
                do_read_from_client(); // 开始转发
            });
    }

    void send_udp_reply(const std::string &host, int port)
    {
        auto self(shared_from_this());
        auto reply = std::make_shared<std::vector<uint8_t>>();

        reply->push_back(0x05); // VER
        reply->push_back(0x00); // REP
        reply->push_back(0x00); // RSV
        reply->push_back(0x01); // ATYP IPv4

        // 简单示例：IPv4
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
            [this, self](std::error_code ec, std::size_t)
            {
                if (ec)
                {
                    print_error("write udp reply error");
                    return;
                }
            });
    }

    // -------- 浏览器 -> P2P隧道 --------
    void do_read_from_client()
    {
        auto self(shared_from_this());
        socket_.async_read_some(
            asio::buffer(client_buf_),
            [this, self](std::error_code ec, std::size_t n)
            {
                if (ec)
                {
                    // print_error("read client error");
                    mux_.send_fin(session_->stream_id());
                    return;
                }
                mux_.send_data(session_->stream_id(), client_buf_.data(), n);
                do_read_from_client();
            });
    }

    void do_read_from_client_for_udp()
    {
        auto self(shared_from_this());
        socket_.async_read_some(
            asio::buffer(client_buf_),
            [this, self](std::error_code ec, std::size_t n)
            {
                if (ec)
                {
                    // print_error("read client error");
                    return;
                }
                do_read_from_client_for_udp();
            });
    }

    // -------- P2P隧道 -> 浏览器 --------
    void do_write_to_client()
    {
        auto self(shared_from_this());
        asio::async_write(
            socket_, asio::buffer(write_queue_.front()),
            [this, self](std::error_code ec, std::size_t)
            {
                if (ec)
                {
                    print_error("write client error");
                    return;
                }
                write_queue_.pop_front();
                if (!write_queue_.empty())
                    do_write_to_client();
            });
    }

    void print_error(const std::string &msg)
    {
        std::cout << "stream_id=" << (session_.get() ? session_->stream_id() : 0) << " " << "host=" << target_host_ << " " << "port=" << target_port_ << " " << msg << std::endl;
    }

    tcp::socket socket_;
    SessionMux &mux_;
    std::array<uint8_t, 512> buf_{};
    std::array<uint8_t, 8192> client_buf_{};
    std::deque<std::vector<uint8_t>> write_queue_;

    std::string target_host_;
    uint16_t target_port_ = 0;
    std::shared_ptr<Session> session_;
    std::shared_ptr<UdpServer> udp_session_;

    asio::io_context &io_;
};

class SocksServer
{
public:
    SocksServer(asio::io_context &io, uint16_t port, SessionMux &mux)
        : acceptor_(io, tcp::endpoint(tcp::v4(), port)), mux_(mux), io_(io)
    {
        std::cout << "[本地] SOCKS5 入口已启动，监听端口 " << port << "\n";
        do_accept();
    }

private:
    void do_accept()
    {
        acceptor_.async_accept([this](std::error_code ec, tcp::socket socket)
                               {
            if (!ec) {
                std::make_shared<ClientSession>(io_, std::move(socket), mux_)->start();
            }
            do_accept(); });
    }
    tcp::acceptor acceptor_;
    SessionMux &mux_;
    asio::io_context &io_;
};
