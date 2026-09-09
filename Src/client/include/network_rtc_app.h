#pragma once
#include <string>
#include <vector>
#include "socks5.h"
#include "p2p_client.h"
#include "session_controller.h"

class NetworkRtcApp
{
public:
    struct Config
    {
        std::string signalingUrl;
        std::vector<std::pair<std::string, uint16_t>> stunServers;
        std::string password;
        uint16_t socks5_server_port;
    };
    using CloseCallback = std::function<void()>;
    NetworkRtcApp(uint32_t peerConnId, asio::io_context &io_context);
    ~NetworkRtcApp() = default;
    void stop();
    void start(const Config &config);
    void onClose(CloseCallback callback){close_callback_ = std::move(callback);};

private:
    uint32_t peer_conn_id_ = 0;
    asio::io_context &io_context_;
    SessionController session_controller_;
    SessionMux mux_;
    SocksServer server_;
    CloseCallback close_callback_;
    Config config_;
};

