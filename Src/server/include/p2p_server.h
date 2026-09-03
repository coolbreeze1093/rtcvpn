#pragma once
#include <rtc/rtc.hpp>
#include <iostream>
#include <memory>
#include <thread>
#include <variant>
#include <nlohmann/json.hpp>
#include <csignal>
#include <plog/Log.h>

using json = nlohmann::json;

class ws_server : public std::enable_shared_from_this<ws_server>
{

public:
    ws_server(uint32_t id_card);

    ~ws_server();

    void connect(std::shared_ptr<rtc::WebSocket> ws);

    void disconnect();

    void send(const std::string &str);

    void bindLoginSuccess(std::function<void(uint32_t)> callback);

    void bindsetRemoteDescriptionFunc(std::function<void(std::string, std::string)> callback);

    void bindaddRemoteCandidateFunc(std::function<void(std::string, std::string)> callback);

    void bindWebSocket(std::shared_ptr<rtc::WebSocket> ws);

    void bindCloseFunc(std::function<void(uint32_t)> cb);

private:
    std::shared_ptr<rtc::WebSocket> ws_;
    std::function<void(std::string, std::string)> setRemoteDescriptionFunc_;
    std::function<void(std::string, std::string)> addRemoteCandidateFunc_;
    std::function<void(uint32_t)> loginSuccessFunc_;
    std::function<void(uint32_t)> close_func_;

    uint32_t id_card_;
};

class p2p_server : public std::enable_shared_from_this<p2p_server>
{
public:
    p2p_server(uint32_t id_card);

    ~p2p_server();

    void init(rtc::Configuration config);

    void send(rtc::message_variant message);

    void send(std::byte *data, size_t size);

    void close();

    void bindCloseFunc(std::function<void(uint32_t)> cb);

    void bindWsSend(std::function<void(std::string)> callback);

    void bindDataChannel(std::function<void(rtc::binary)> callback);

    void setRemoteDescription(std::string sdp, std::string type);

    void addRemoteCandidate(std::string candidate, std::string mid);

    void createDataChannel(std::shared_ptr<rtc::DataChannel> dc);

    void createPeerConnection();

private:
    std::shared_ptr<rtc::DataChannel> dc_;
    std::shared_ptr<rtc::PeerConnection> pc_;
    uint32_t id_card_;
    rtc::Configuration p2p_config_;
    std::function<void(rtc::binary message)> data_channel_binary_callback_;
    std::function<void(const std::string &)> send_func_;
    std::function<void(uint32_t)> close_func_;
};