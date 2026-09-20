#pragma once
#include <rtc/rtc.hpp>
#include <string>
#include <functional>
#include <memory>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// 只负责 WebSocket 信令通道的收发，不感知 WebRTC 细节
class SignalingClient
{
public:
    // 收到一条已解析的信令消息时回调（type + 完整 json）
    using MessageCallback = std::function<void(const json &message)>;
    using StateCallback   = std::function<void()>;
    using ErrorCallback   = std::function<void(const std::string &error)>;

    void connect(const std::string &url);
    void connect(std::shared_ptr<rtc::WebSocket> ws);

    void disconnect();

    // 发送任意信令 json（由 P2PClient 通过回调传入的内容转发）
    void send(const json &message);

    void onMessage(MessageCallback cb) { message_callback_ = std::move(cb); }
    void onOpen(StateCallback cb)      { open_callback_ = std::move(cb); }
    void onClosed(StateCallback cb)    { closed_callback_ = std::move(cb); }
    void onError(ErrorCallback cb)     { error_callback_ = std::move(cb); }

    bool isOpen() const { return ws_ && ws_->isOpen(); }

private:
    void bindWebSocket();

    std::shared_ptr<rtc::WebSocket> ws_;

    MessageCallback message_callback_;
    StateCallback   open_callback_;
    StateCallback   closed_callback_;
    ErrorCallback   error_callback_;
};