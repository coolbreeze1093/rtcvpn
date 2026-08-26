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
                        std::cout << "WebSocket opened" << std::endl; });

        ws->onMessage([](rtc::binary data) {}, [this](std::string message)
                      {
                        json message_json = json::parse(message);

                        if(message_json.find("type")==message_json.end())
                        {
                            std::cout << "Invalid message" << std::endl;
                            return;
                        }
                        
                        std::string type = message_json["type"];
                        if(type == "answer")
                        {
                            if(message_json.find("description")==message_json.end())
                            {
                                std::cout << "No sdp" << std::endl;
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
                                    std::cout << "Result failed" << std::endl;
                                    return;
                                }
                                this->createPeerConnection();
                                this->createDataChannel();
                            }
                            else
                            {
                                std::cout << "Error: No result field in Verify message" << std::endl;
                            }
                        }
                        else if(type == "candidate")
                        {
                            if(message_json.find("candidate") == message_json.end())
                            {
                                std::cerr << "Error: No candidate field in Candidate message" << std::endl;
                                return;
                            }
                            if(message_json.find("mid") == message_json.end())
                            {
                                std::cerr << "Error: No mid field in Candidate message" << std::endl;
                                return;
                            }

                            std::string candidate = message_json["candidate"];
                            std::string mid = message_json["mid"];
                            this->pc_->addRemoteCandidate(rtc::Candidate(candidate, mid));
                        }
                        else
                        {
                                // Invalid message
                            std::cout << "Invalid message" << std::endl;
                            return;
                        }

                        std::cout << "Received: "
                                   << message
                                   << std::endl; });
        ws->onClosed([]()
                     { std::cout << "WebSocket closed" << std::endl; });

        ws->onError([](std::string message)
                    { std::cout << "WebSocket error: "
                                << message
                                << std::endl; });
    }

    void createDataChannel()
    {
        dc_ = pc_->createDataChannel("data");
        dc_->onOpen([this]()
                    { std::cout << "DataChannel opened" << std::endl; });

        dc_->onClosed([]()
                      { std::cout << "DataChannel closed" << std::endl; });

        dc_->onError([](std::string message)
                     { std::cout << "DataChannel error: " << message << std::endl; });

        dc_->onMessage([this](rtc::binary message)
                       {
            if(data_channel_binary_callback_)
            {
                data_channel_binary_callback_(message);
            }
            else
            {
                std::cout << "rev Data data_channel_binary_callback_ is null" << std::endl;
            } }, [this](std::string message)
                       { std::cout << "DataChannel message: " << message << std::endl; });
    }

    void createPeerConnection()
    {
        pc_ = std::make_shared<rtc::PeerConnection>(p2p_config_);

        pc_->onStateChange([this](rtc::PeerConnection::State state)
                           { 
                            if(state == rtc::PeerConnection::State::Connected)
                            {
                                
                            }

                            std::cout << "PeerConnection state: "
                                       << static_cast<int>(state)
                                       << std::endl; });

        pc_->onGatheringStateChange([](rtc::PeerConnection::GatheringState state)
                                    { std::cout << "Gathering state: "
                                                << static_cast<int>(state)
                                                << std::endl; });

        pc_->onLocalCandidate([this](rtc::Candidate candidate)
                              { 
                                json j;
                                j["type"] = "candidate";
                                j["candidate"] = candidate;
                                j["mid"] = candidate.mid();
                                this->ws_->send(j.dump());
                                std::cout << "Local candidate: "
                                          << candidate
                                          << std::endl; });

        pc_->onLocalDescription([this](rtc::Description description)
                                { 
                                    json j;
                                    j["type"]="offer";
                                    j["description"]=description;
                                    this->ws_->send(j.dump());
                                    std::cout << "Local description: "
                                            << description
                                            << std::endl; });
    }

private:
    std::shared_ptr<rtc::DataChannel> dc_;
    std::shared_ptr<rtc::PeerConnection> pc_;
    std::shared_ptr<rtc::WebSocket> ws_;
    rtc::Configuration p2p_config_;

    std::function<void(rtc::binary message)> data_channel_binary_callback_;
};