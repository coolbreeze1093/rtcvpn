#pragma once
#include <rtc/rtc.hpp>
#include <iostream>
#include <memory>
#include <thread>
#include <variant>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class p2p_client
{

public:
    void init(rtc::Configuration config);

    void connect(std::string url);

    void send(rtc::message_variant message);

    void send(const std::byte *data, size_t size);

    void disconnect();

    void bindDataChannel(std::function<void(rtc::binary)> callback);

    void bindWebSocket(std::shared_ptr<rtc::WebSocket> ws);

    void createDataChannel();

    void createPeerConnection();

private:
    std::shared_ptr<rtc::DataChannel> dc_;
    std::shared_ptr<rtc::PeerConnection> pc_;
    std::shared_ptr<rtc::WebSocket> ws_;
    rtc::Configuration p2p_config_;

    std::function<void(rtc::binary message)> data_channel_binary_callback_;
};