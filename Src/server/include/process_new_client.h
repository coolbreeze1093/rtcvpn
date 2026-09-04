#pragma once
#include "p2p_server.h"
#include "session_mux.h"
#include "tcp_session.h"
#include "udp_session.h"

class Socks5Session : public std::enable_shared_from_this<Socks5Session>
{
public:
    Socks5Session(asio::io_context &io,
                  rtc::Configuration &config,
                  uint32_t session_id);

    ~Socks5Session();

    void start(std::shared_ptr<rtc::WebSocket> ws);

    void bindCloseFunc(std::function<void(uint32_t)> cb);

    uint32_t id() const;

private:
    void onLoginSuccess();

    void notifyClose();

    asio::io_context &io_;
    rtc::Configuration &config_;
    uint32_t session_id_;

    std::shared_ptr<SessionMux> mux_; // 独立的 mux，每个 session 一份

    std::shared_ptr<ws_server> ws_;
    std::shared_ptr<p2p_server> p2p_;

    std::unordered_map<uint32_t, std::shared_ptr<TcpSession>> tcp_sessions_;
    std::unordered_map<uint32_t, std::shared_ptr<UdpClient>> udp_sessions_;

    std::function<void(uint32_t)> close_cb_;
};

class ProcessNewWsClient
{
public:
    ProcessNewWsClient(asio::io_context &io, rtc::Configuration config);

    ~ProcessNewWsClient();

    void newClient(std::shared_ptr<rtc::WebSocket> ws);

    uint32_t create_session_id();

private:
    asio::io_context &io_;
    rtc::Configuration config_;

    std::unordered_map<uint32_t, std::shared_ptr<Socks5Session>> client_sessions_;
    uint32_t id_ = 0;
};