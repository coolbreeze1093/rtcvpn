#pragma once
#include <string>
#include <vector>
#include "socks5.h"
#include "p2p_client.h"
#include "p2p_session_controller.h"
#include "client_config.h"

class NetworkRtcApp
{
public:
    
    using CloseCallback = std::function<void()>;
    NetworkRtcApp(uint32_t peerConnId, asio::io_context &io_context);
    ~NetworkRtcApp() = default;
    void stop();
    void start(const ClientConfig &config);
    void onClose(CloseCallback callback){close_callback_ = std::move(callback);};

private:
    uint32_t peer_conn_id_ = 0;
    asio::io_context &io_context_;
    P2PSessionController p2p_session_controller_;
    SessionMux mux_;
    SocksServer server_;
    CloseCallback close_callback_;
    ClientConfig config_;
};

