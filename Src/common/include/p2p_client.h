#pragma once
#include <rtc/rtc.hpp>
#include <functional>
#include <memory>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// 只负责 PeerConnection / DataChannel，不知道信令是怎么传输的
class P2PClient
{
public:
    using BinaryMessageCallback = std::function<void(rtc::binary)>;
    // 需要通过信令通道发送出去的消息（offer / candidate），由外部转发
    using SignalOutCallback     = std::function<void(const json &message)>;
    using StateCallback         = std::function<void(rtc::PeerConnection::State)>;

    void init(rtc::Configuration config);

    // 外部信令收到消息后调用此接口喂给 P2PClient 处理
    void handleSignalMessage(const json &message);

    // 首次协商发起：创建 PeerConnection + DataChannel 并开始 offer 流程
    void start();

    void send(rtc::message_variant message);
    void send(const std::byte *data, size_t size);

    void close();

    void bindDataChannel(BinaryMessageCallback callback) { data_channel_binary_callback_ = std::move(callback); }
    void onSignalOut(SignalOutCallback callback)          { signal_out_callback_ = std::move(callback); }
    void onStateChange(StateCallback callback)            { state_change_callback_ = std::move(callback); }

private:
    void createDataChannel();
    void createPeerConnection();

    std::shared_ptr<rtc::DataChannel> dc_;
    std::shared_ptr<rtc::PeerConnection> pc_;
    rtc::Configuration p2p_config_;

    BinaryMessageCallback data_channel_binary_callback_;
    SignalOutCallback     signal_out_callback_;
    StateCallback         state_change_callback_;
};