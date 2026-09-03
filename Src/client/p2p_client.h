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
    void init(rtc::Configuration config)
    {
        p2p_config_ = config;
    }
    void connect(std::string url)
    {

        ws_ = std::make_shared<rtc::WebSocket>();
        bindWebSocket(ws_);
        ws_->open(url);
    }

    void send(rtc::message_variant message)
    {
        if (dc_)
            dc_->send(message);
    }

    void send(const std::byte *data, size_t size)
    {
        if (dc_)
            dc_->send(data, size);
    }

    void disconnect()
    {
        ws_->close();
    }

    void bindDataChannel(std::function<void(rtc::binary)> callback)
    {
        data_channel_binary_callback_ = callback;
    }

    void bindWebSocket(std::shared_ptr<rtc::WebSocket> ws)
    {
        ws->onOpen([this]()
                   {
                        json message;
                        message["type"] = "verify";
                        message["passwd"] = "test";
                        ws_->send(message.dump());
                        PLOG_INFO << "WebSocket opened"; });

        ws->onMessage([](rtc::binary data) {}, [this](std::string message)
                      {
                        json message_json = json::parse(message);

                        if(message_json.find("type")==message_json.end())
                        {
                            PLOG_ERROR << "Invalid message";
                            return;
                        }
                        
                        std::string type = message_json["type"];
                        if(type == "answer")
                        {
                            if(message_json.find("description")==message_json.end())
                            {
                                PLOG_ERROR << "No sdp";
                                return;
                            }
                            std::string sdp = message_json["description"];
                            this->pc_->setRemoteDescription(rtc::Description(sdp,type));
                        }
                        else if(type == "verify")
                        {
                            if(message_json.find("result") != message_json.end())
                            {
                                std::string result = message_json["result"];
                                if(result == "failed")
                                { 
                                    PLOG_ERROR << "Result failed";
                                    return;
                                }
                                this->createPeerConnection();
                                this->createDataChannel();
                            }
                            else
                            {
                                PLOG_ERROR << "Error: No result field in Verify message";
                            }
                        }
                        else if(type == "candidate")
                        {
                            if(message_json.find("candidate") == message_json.end())
                            {
                                PLOG_ERROR << "Error: No candidate field in Candidate message";
                                return;
                            }
                            if(message_json.find("mid") == message_json.end())
                            {
                                PLOG_ERROR << "Error: No mid field in Candidate message";
                                return;
                            }

                            std::string candidate = message_json["candidate"];
                            std::string mid = message_json["mid"];
                            this->pc_->addRemoteCandidate(rtc::Candidate(candidate, mid));
                        }
                        else
                        {
                                // Invalid message
                            PLOG_ERROR << "Invalid message";
                            return;
                        }

                        PLOG_INFO << "Received: "
                                   << message
                                   << std::endl; });
        ws->onClosed([this]()
                     { 
                        pc_->close();
                        PLOG_INFO << "WebSocket closed"; });

        ws->onError([](std::string message)
                    { PLOG_ERROR << "WebSocket error: "
                                << message; });
    }

    void createDataChannel()
    {
        dc_ = pc_->createDataChannel("data");
        dc_->onOpen([this]()
                    { PLOG_INFO << "DataChannel opened"; });

        dc_->onClosed([]()
                      { PLOG_INFO << "DataChannel closed"; });

        dc_->onError([](std::string message)
                     { PLOG_ERROR << "DataChannel error: " << message; });

        dc_->onMessage([this](rtc::binary message)
                       {
            if(data_channel_binary_callback_)
            {
                data_channel_binary_callback_(message);
            }
            else
            {
                PLOG_INFO << "rev Data data_channel_binary_callback_ is null";
            } }, [this](std::string message)
                       { PLOG_INFO << "DataChannel message: " << message; });
    }

    void createPeerConnection()
    {
        pc_ = std::make_shared<rtc::PeerConnection>(p2p_config_);

        pc_->onStateChange([this](rtc::PeerConnection::State state)
                           { 
                            if(state == rtc::PeerConnection::State::Connected)
                            {
                                
                            }

                            PLOG_INFO << "PeerConnection state: "
                                       << static_cast<int>(state); });

        pc_->onGatheringStateChange([](rtc::PeerConnection::GatheringState state)
                                    { PLOG_INFO << "Gathering state: "
                                                << static_cast<int>(state); });

        pc_->onLocalCandidate([this](rtc::Candidate candidate)
                              { 
                                json j;
                                j["type"] = "candidate";
                                j["candidate"] = candidate;
                                j["mid"] = candidate.mid();
                                this->ws_->send(j.dump());
                                PLOG_INFO << "Local candidate: "
                                          << candidate; });

        pc_->onLocalDescription([this](rtc::Description description)
                                { 
                                    json j;
                                    j["type"]="offer";
                                    j["description"]=description;
                                    this->ws_->send(j.dump());
                                    PLOG_INFO << "Local description: "
                                            << description; });
    }

private:
    std::shared_ptr<rtc::DataChannel> dc_;
    std::shared_ptr<rtc::PeerConnection> pc_;
    std::shared_ptr<rtc::WebSocket> ws_;
    rtc::Configuration p2p_config_;

    std::function<void(rtc::binary message)> data_channel_binary_callback_;
};