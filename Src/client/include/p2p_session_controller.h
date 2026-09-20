#pragma once
#include "p2p_client.h"
#include "signaling.h"
#include <plog/Log.h>

class P2PSessionController
{
public:
    enum class State
    {
        Connected,
        Closed,
    };
    using OnStateChangeCallback = std::function<void(P2PSessionController::State state)>;
    using OnLoginSuccessCallback = std::function<void()>;

    P2PSessionController() = default;

    ~P2PSessionController() {

    };

    void init(rtc::Configuration config, std::string password)
    {
        password_ = std::move(password);
        p2p_.init(config);

        // P2P 产生的 offer/candidate -> 通过信令发出去
        p2p_.onSignalOut([this](const json &msg)
                         { signaling_.send(msg); });

        p2p_.onStateChange([this](rtc::PeerConnection::State state)
                           {
            PLOG_INFO << "P2PClient state change: " << state;

            switch(state)
            {
                case rtc::PeerConnection::State::Connected:
                    if(state_change_callback_)
                    {
                        state_change_callback_(State::Connected);
                    }
                    p2p_connected_ = true;
                    PLOG_INFO << "P2PClient connected";
                    break;
                case rtc::PeerConnection::State::Disconnected:
                    PLOG_INFO << "P2PClient disconnected";
                    break;
                case rtc::PeerConnection::State::Closed:
                    PLOG_INFO << "P2PClient closed";
                    if(state_change_callback_)
                    {
                        state_change_callback_(State::Closed);
                    }
                    p2p_connected_ = false;
                    signaling_.disconnect();
                    break;
                case rtc::PeerConnection::State::Connecting:
                    PLOG_INFO << "P2PClient connecting";
                    break;
                case rtc::PeerConnection::State::Failed:
                    PLOG_INFO << "P2PClient failed";
                    if(state_change_callback_)
                    {
                        state_change_callback_(State::Closed);
                    }
                    p2p_connected_ = false;
                    signaling_.disconnect();
                    break;
                default:
                    PLOG_ERROR << "P2PClient unknown state";
                    break;
            } });

        // 信令收到的消息 -> 分发给业务逻辑 / P2P
        signaling_.onMessage([this](const json &msg)
                             {
            std::string type = msg.value("type", "");
            if (type == "verifyACK")
            {
                if (msg.find("result") != msg.end() && msg["result"] == "success")
                {
                    p2p_.createPeerConnection();
                    p2p_.createDataChannel("data");
                }
                else
                {
                    PLOG_ERROR << "verify failed!";
                    signaling_.disconnect();
                }
            }
            else if (type == "verify")
            {
                if(msg.find("passwd")!=msg.end())
                        {
                            std::string pwd = msg["passwd"];
                            if(pwd == password_)
                            {
                                PLOG_DEBUG << "Password is correct";
                                json j;
                                j["type"] = "verifyACK";
                                j["result"] = "success";
                                p2p_.createPeerConnection();
                                login_success_callback_();
                                signaling_.send(j.dump());
                            }
                            else
                            {
                                PLOG_ERROR << "Password is incorrect";
                                json j;
                                j["type"] = "verifyACK";
                                j["result"] = "failed";
                                signaling_.send(j.dump());
                            }
                        }
                        else
                        {
                            PLOG_ERROR << "Password is incorrect";
                            json j;
                            j["status"] = "failed";
                            j["type"] = "verifyACK";
                            signaling_.send(j.dump());
                        }
            }
            else
            {
                p2p_.handleSignalMessage(msg);
            } });

        signaling_.onOpen([this]()
                          {
            json verify_msg;
            verify_msg["type"] = "verify";
            verify_msg["passwd"] = password_;
            signaling_.send(verify_msg); });

        signaling_.onClosed([this]()
                            {
            p2p_.close();
            if(!p2p_connected_)
            {
                PLOG_INFO << "signaling_ closed";
                if(state_change_callback_)
                {
                    state_change_callback_(State::Closed);
                }
            } });
    }

    void connect(const std::string &url) { signaling_.connect(url); }
    void connect(std::shared_ptr<rtc::WebSocket> ws) { signaling_.connect(std::move(ws)); }
    void disconnect() { signaling_.disconnect(); }

    void send(rtc::message_variant message) { p2p_.send(message); }
    void send(const std::byte *data, size_t len) { p2p_.send(data, len); }
    void bindDataChannel(P2PClient::BinaryMessageCallback cb) { p2p_.bindDataChannel(std::move(cb)); }

    void onStateChange(OnStateChangeCallback cb) { state_change_callback_ = std::move(cb); }

private:
    SignalingClient signaling_;
    P2PClient p2p_;
    std::string password_;
    OnStateChangeCallback state_change_callback_;
    OnLoginSuccessCallback login_success_callback_;
    bool p2p_connected_ = false;
};