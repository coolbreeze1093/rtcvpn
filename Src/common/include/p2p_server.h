#pragma once
#include <rtc/rtc.hpp>
#include <iostream>
#include <memory>
#include <thread>
#include <variant>
#include <nlohmann/json.hpp>
#include <csignal>

using json = nlohmann::json;

class ws_server:public std::enable_shared_from_this<ws_server>
{
    
public:
    ws_server(uint32_t id_card)
    {
        id_card_ = id_card;
    }
    ~ws_server()
    {
        std::cout << "ws_server::~ws_server()  " << id_card_<< std::endl;
    }
    void connect(std::shared_ptr<rtc::WebSocket> ws)
    {
        ws_ = ws;
        bindWebSocket(ws_);
    }
    void disconnect()
    {
        ws_->close();
    }

    void send(const std::string& str)
    {
        ws_->send(str);
    }

    void bindLoginSuccess(std::function<void(uint32_t)> callback)
    {
        loginSuccessFunc_ = callback;
    }

    void bindsetRemoteDescriptionFunc(std::function<void(std::string,std::string)> callback)
    {
        setRemoteDescriptionFunc_ = callback;
    }

    void bindaddRemoteCandidateFunc(std::function<void(std::string,std::string)> callback)
    {
        addRemoteCandidateFunc_ = callback;
    }
    void bindWebSocket(std::shared_ptr<rtc::WebSocket> ws)
    {
        ws->onOpen([this]()
                   { std::cout << "WebSocket opened" << std::endl; });

        ws->onMessage([](rtc::binary data) {}, [this](std::string message)
                      {
                        try {
                        std::cout << "Received message: " << message << std::endl;
                        json message_json = json::parse(message);

                        if(message_json.find("type")==message_json.end())
                        {
                            std::cout << "Invalid message" << std::endl;
                            return;
                        }
                        
                        std::string type = message_json["type"];
                        if(type == "offer")
                        {
                            if(message_json.find("description")==message_json.end())
                            {
                                std::cout << "No sdp" << std::endl;
                                return;
                            }
                            std::string sdp = message_json["description"];
                            this->setRemoteDescriptionFunc_(sdp,type);
                            //this->pc_->setRemoteDescription(rtc::Description(sdp,type));
                        }
                        else if(type == "verify")
                        {
                            if(message_json.find("passwd")!=message_json.end())
                            {
                                std::string pwd = message_json["passwd"];
                                if(pwd == "test")
                                {
                                    std::cout << "Password is correct" << std::endl;
                                    json j;
                                    j["type"] = "verify";
                                    j["result"] = "success";
                                    this->loginSuccessFunc_(id_card_);
                                    this->ws_->send(j.dump());
                                }
                            }
                            else
                            {
                                std::cout << "Password is incorrect" << std::endl;
                                json j;
                                j["status"] = "failed";
                                j["type"] = "verify";
                                this->ws_->send(j.dump());
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
                            std::string mid = message_json["mid"].get<std::string>();
                            this->addRemoteCandidateFunc_(candidate,mid);
                            //this->pc_->addRemoteCandidate(rtc::Candidate(candidate, mid));
                        }
                        else
                        {
                                // Invalid message
                            std::cout << "Invalid message" << std::endl;
                            return;
                        }
                    }
                        catch (const std::exception& e) {
            std::cout << "WebSocket message exception: "
                      << e.what() << std::endl;
        }

                        std::cout << "Received: "
                                   << message
                                   << std::endl; });
        ws->onClosed([this]()
                     { 
                        close_func_(id_card_);
                        std::cout << "WebSocket closed" << std::endl; });

        ws->onError([](std::string message)
                    {
                        std::cout << "WebSocket error: "
                                << message
                                << std::endl; });
    }

    void bindCloseFunc(std::function<void (uint32_t)> cb)
    {
        close_func_ = std::move(cb);
    }


private:
    std::shared_ptr<rtc::WebSocket> ws_;
    std::function<void (std::string,std::string)> setRemoteDescriptionFunc_;
    std::function<void (std::string,std::string)> addRemoteCandidateFunc_;
    std::function<void (uint32_t)> loginSuccessFunc_;
    std::function<void (uint32_t)> close_func_;

    uint32_t id_card_;
};

class p2p_server:public std::enable_shared_from_this<p2p_server>
{
public:
    p2p_server(uint32_t id_card)
    {
        id_card_ = id_card;
    }

    ~p2p_server(){
        std::cout<<"~p2p_server"<<std::endl;
    }
    void init(rtc::Configuration config)
    {
        p2p_config_ = config;
        createPeerConnection();
    }
    void send(rtc::message_variant message)
    {
        dc_->send(message);
    }
    void send(std::byte *data, size_t size)
    {
        dc_->send(data, size);
    }

    void bindCloseFunc(std::function<void (uint32_t)> cb)
    {
        close_func_ = std::move(cb);
    }

    void bindWsSend(std::function<void(std::string)> callback) {

        send_func_ = std::move(callback);
    }
    void bindDataChannel(std::function<void(rtc::binary)> callback)
    {
        data_channel_binary_callback_ = callback;
    }

    void setRemoteDescription(std::string sdp,std::string type)
    {
        pc_->setRemoteDescription(rtc::Description(sdp,type));
    }

    void addRemoteCandidate(std::string candidate,std::string mid)
    {
        pc_->addRemoteCandidate(rtc::Candidate(candidate, mid));
    }
    void createDataChannel(std::shared_ptr<rtc::DataChannel> dc)
    {
        dc_ = dc;
        dc_->onOpen([]()
                    { std::cout << "DataChannel opened" << std::endl; });

        dc_->onClosed([]()
                      { std::cout << "DataChannel closed" << std::endl; });

        dc_->onError([](std::string message)
                     { std::cout << "DataChannel error: "; });

        dc_->onMessage([this](rtc::binary message)
                       {
            if(this->data_channel_binary_callback_)
            {
                this->data_channel_binary_callback_(message);
            }
            else
            {
                std::cout << "DataChannel binary message: " << message.size() << " bytes" << std::endl;
            } }, [this](std::string message)
                       { std::cout << "DataChannel message: " << message << std::endl; });
    }

    void createPeerConnection()
    {
        pc_ = std::make_shared<rtc::PeerConnection>(p2p_config_);

        pc_->onStateChange([this](rtc::PeerConnection::State state)
                           { 
                            if(state == rtc::PeerConnection::State::Closed)
                            {
                                close_func_(id_card_);
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
                                this->send_func_(j.dump());
                                //this->ws_->send(j.dump());
                                std::cout << "Local candidate: "
                                          << candidate
                                          << std::endl; });

        pc_->onLocalDescription([this](rtc::Description description)
                                { 
                                    json j;
                                    j["type"]="answer";
                                    j["description"]=description;
                                    this->send_func_(j.dump());
                                    //this->ws_->send(j.dump());
                                    std::cout << "Local description: "
                                            << description
                                            << std::endl; });

        pc_->onDataChannel([this](std::shared_ptr<rtc::DataChannel> dc)
                           {
                              std::cout << "New DataChannel" << std::endl;
                              createDataChannel(dc); });
    }

    

private:
    std::shared_ptr<rtc::DataChannel> dc_;
    std::shared_ptr<rtc::PeerConnection> pc_;
    uint32_t id_card_;
    rtc::Configuration p2p_config_;
    std::function<void(rtc::binary message)> data_channel_binary_callback_;
    std::function<void (const std::string&)> send_func_;
    std::function<void (uint32_t)> close_func_;
};