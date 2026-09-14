#pragma once
#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include "session_mux.h"
#include "udp_session.h"
#include "http_parse.h"

using asio::ip::tcp;

class Socks5Session : public std::enable_shared_from_this<Socks5Session>
{
    enum class State
    {
        Greeting,
        TcpConnecting,
        TcpConnected,
        UdpConnecting,
        UdpConnected,
        HttpConnecting,
        HttpConnected,
        HttpsConnecting,
        HttpsConnected,
    };
public:
    Socks5Session(asio::io_context &io, tcp::socket socket, SessionMux &mux, uint32_t session_id);

    void start();

    ~Socks5Session();

    void set_on_close(std::function<void(uint32_t)> on_close);

    void close();

private:
    std::array<uint8_t, 8192> request_buf_;

    void do_read_greeting();

    void do_read_http_request_line();

    void request_remote_connect_for_http();

    void http_connected();

    void do_read_from_client_for_http();

    void do_connect_upstream_and_tunnel_for_https(bool ok);

    void request_remote_connect_for_https();

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

    void write_to_client(const uint8_t *data, size_t size);

    // -------- P2P隧道 -> 浏览器 --------

    void write_to_client(std::vector<uint8_t> v);
    void do_write_to_client();

    void print_error(const std::string &msg);

    void close_session();

    tcp::socket socket_;
    SessionMux &mux_;
    std::array<uint8_t, 512> buf_{};
    std::array<uint8_t, 8192> client_buf_{};
    std::deque<std::vector<uint8_t>> write_queue_;

    std::string target_host_;
    uint16_t target_port_ = 0;
    std::shared_ptr<Session> session_ = nullptr;
    std::shared_ptr<UdpSession> udp_session_ = nullptr;

    asio::io_context &io_;

    std::function<void(uint32_t)> on_close_;

    uint32_t session_id_ = 0;
    std::mutex mutex_;
    bool is_closed_{false};
    
    p2psocks::HttpParser http_response_parser_;
    p2psocks::HttpParser http_request_parser_;

    State state_ = State::Greeting;

    bool request_line_done_ = false;

    p2psocks::CtrlType ctrl_type_ = p2psocks::CtrlType::receive;

    bool is_writing_{false};
};