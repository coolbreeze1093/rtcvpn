#include "network_rtc_app.h"

NetworkRtcApp::NetworkRtcApp(uint32_t peerConnId, asio::io_context &io_context)
    : peer_conn_id_(peerConnId), io_context_(io_context), mux_(peer_conn_id_), server_(io_context_, mux_)
{
    p2p_session_controller_.bindDataChannel([this](rtc::binary data)
                                            {
                        auto result = p2psocks::unpackMessage(data.data(), data.size());
                        if (!result)
                        {
                            return;
                        }

                    mux_.on_p2p_data(1, result->payload, result->len); });
    mux_.set_send_func([this](uint32_t conn_id, const uint8_t *data, size_t len)
                       {
            std::vector<std::byte> buf;
            try
            {
                buf = p2psocks::packMessage(data, len);
            }
            catch (const std::length_error &e)
            {
                PLOG_ERROR << "sendData: " << e.what();
                return;
            }
            p2p_session_controller_.send(buf.data(), buf.size()); });
}

void NetworkRtcApp::stop()
{
    PLOG_DEBUG << "NetworkRtcApp stoping";
    server_.stop();
    p2p_session_controller_.disconnect();
    PLOG_DEBUG << "NetworkRtcApp stoped";
}

void NetworkRtcApp::start(const ClientConfig &config)
{
    config_ = config;
    rtc::Configuration rtc_config;
    rtc_config.iceServers.push_back({config.stun_ip, config.stun_port});
    if (!config.ws_proxy_server.empty())
    {
        rtc_config.proxyServer = {config.ws_proxy_server};
    }

    rtc::WebSocketConfiguration ws_config;
    ws_config.disableTlsVerification = config_.ws_disable_tls_verification;
    if (!config_.ws_proxy_server.empty())
    {
        ws_config.proxyServer = {config_.ws_proxy_server};
    }
    if (!config_.ws_protocols.empty())
    {
        ws_config.protocols = {config_.ws_protocols};
    }
    if (config_.ws_connection_timeout > 0)
    {
        ws_config.connectionTimeout = std::chrono::milliseconds(config_.ws_connection_timeout);
    }
    if (config_.ws_ping_interval > 0)
    {
        ws_config.pingInterval = std::chrono::milliseconds(config_.ws_ping_interval);
    }
    if (config_.ws_max_outstanding_pings > 0)
    {
        ws_config.maxOutstandingPings = config_.ws_max_outstanding_pings;
    }
    if (!config_.ws_ca_certificate_pem_file.empty())
    {
        ws_config.caCertificatePemFile = config_.ws_ca_certificate_pem_file;
    }
    if (!config_.ws_certificate_pem_file.empty())
    {
        ws_config.certificatePemFile = config_.ws_certificate_pem_file;
    }
    if (!config_.ws_key_pem_file.empty())
    {
        ws_config.keyPemFile = config_.ws_key_pem_file;
    }
    if (!config_.ws_key_pem_pass.empty())
    {
        ws_config.keyPemPass = config_.ws_key_pem_pass;
    }
    if (config_.ws_max_message_size > 0)
    {
        ws_config.maxMessageSize = config_.ws_max_message_size;
    }

    p2p_session_controller_.init(rtc_config, config_.server_password);

    p2p_session_controller_.onStateChange([this](P2PSessionController::State state)
                                          {   
        switch(state)
        {
        case P2PSessionController::State::Connected:
        {
            PLOG_DEBUG << "SessionController::State::Connected";
            server_.start(config_.socks5_bind_port);
        }
        break;

        case P2PSessionController::State::Closed:
        {
            PLOG_DEBUG << "SessionController::State::Closed";
            server_.stop();
            close_callback_();
        } 
        break;
        default:
        {
            PLOG_ERROR << "SessionController::State::Unknown";
        }
        break;
        
    } });

    p2p_session_controller_.connect(config_.ws_server_ip, ws_config);
}