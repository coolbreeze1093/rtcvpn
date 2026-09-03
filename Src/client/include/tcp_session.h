#pragma once
#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include "session_mux.h"
#include "udp_session.h"

using asio::ip::tcp;
using namespace p2psocks;


class Socks5Session : public std::enable_shared_from_this<Socks5Session>
{
public:
    Socks5Session(asio::io_context &io, tcp::socket socket, SessionMux &mux, uint32_t session_id);

    void start();

    ~Socks5Session();

    void set_on_close(std::function<void(uint32_t)> on_close);

private:
    asio::streambuf request_buf_;

    void do_read_greeting();

    void do_read_http_request_line();

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
                    self->close();
                    return;
                }
                // request_buf_ 中此时已包含全部头部，直接丢弃（consume）
                request_buf_.consume(request_buf_.size());
                handler();
            });
    }

    void do_connect_upstream_and_tunnel_for_http(bool ok);

    void request_remote_connect_for_http();

    void do_read_request();

    void read_udp();

    void request_remote_connect_for_udp();

    void do_connect_upstream_and_tunnel_for_udp(bool ok);

    void read_ipv4();

    void read_domain();

    // -------- 通过 P2P 隧道请求远端建立到目标的连接 --------
    void request_remote_connect();

    void send_socks_reply(uint8_t rep_code);

    // tcp通道回复udp建立信息

    void send_udp_reply(const std::string &host, int port);

    // -------- 浏览器 -> P2P隧道 --------
    void do_read_from_client();
    // 保持tcp不关闭，等待udp数据
    void do_read_from_client_for_udp();

    // -------- P2P隧道 -> 浏览器 --------
    void do_write_to_client();

    void print_error(const std::string &msg);

    void close();

    tcp::socket socket_;
    SessionMux &mux_;
    std::array<uint8_t, 512> buf_{};
    std::array<uint8_t, 8192> client_buf_{};
    std::deque<std::vector<uint8_t>> write_queue_;

    std::string target_host_;
    uint16_t target_port_ = 0;
    std::shared_ptr<Session> session_;
    std::shared_ptr<UdpSession> udp_session_;

    asio::io_context &io_;

    std::function<void(uint32_t)> on_close_;

    uint32_t session_id_ = 0;
    std::mutex mutex_;
    bool is_closed_{false};
};