#include "network_rtc_app.h"

NetworkRtcApp::NetworkRtcApp(uint32_t peerConnId, asio::io_context &io_context)
    : peer_conn_id_(peerConnId)
    , io_context_(io_context)
    , mux_(peer_conn_id_)
    , server_(io_context_, mux_)
{
    session_controller_.bindDataChannel([this](rtc::binary data)
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
            session_controller_.send(buf.data(), buf.size()); });
}

void NetworkRtcApp::stop()
{
    PLOG_DEBUG << "NetworkRtcApp stoping";
    server_.stop();
    session_controller_.disconnect();
    PLOG_DEBUG << "NetworkRtcApp stoped";
}

void NetworkRtcApp::start(const Config &config)
{
    config_ = config;
    rtc::Configuration rtc_config;
    for (const auto &[host, port] : config.stunServers)
    {
        rtc_config.iceServers.push_back({host, port});
    }

    session_controller_.init(rtc_config, config.password);
    session_controller_.connect(config.signalingUrl);
    session_controller_.onStateChange([this](SessionController::State state)
                                      {
        if(state == SessionController::State::Connected)
        {
            PLOG_DEBUG << "SessionController::State::Connected";
            server_.start(config_.socks5_server_port);
        }
        else
        {
            PLOG_DEBUG << "SessionController::State::Disconnected";
            server_.stop();
            close_callback_();
        } });
}