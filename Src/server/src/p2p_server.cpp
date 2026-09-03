#include "p2p_server.h"
#include <plog/Log.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

ws_server::ws_server(uint32_t id_card)
{
    id_card_ = id_card;
    PLOG_DEBUG << "ws_server created  " << id_card_;
}

ws_server::~ws_server()
{
    PLOG_DEBUG << "ws_server::~ws_server()  " << id_card_;
}

void ws_server::connect(std::shared_ptr<rtc::WebSocket> ws)
{
    ws_ = ws;
    bindWebSocket(ws_);
}

void ws_server::disconnect()
{
    ws_->close();
}

void ws_server::send(const std::string &str)
{
    ws_->send(str);
}

void ws_server::bindLoginSuccess(std::function<void(uint32_t)> callback)
{
    loginSuccessFunc_ = callback;
}

void ws_server::bindsetRemoteDescriptionFunc(std::function<void(std::string, std::string)> callback)
{
    setRemoteDescriptionFunc_ = callback;
}

void ws_server::bindaddRemoteCandidateFunc(std::function<void(std::string, std::string)> callback)
{
    addRemoteCandidateFunc_ = callback;
}

void ws_server::bindWebSocket(std::shared_ptr<rtc::WebSocket> ws)
{
    ws->onOpen([this]()
               { PLOG_DEBUG << "WebSocket opened"; });

    ws->onMessage([](rtc::binary data) {}, [this](std::string message)
                  {
                    try {
                    PLOG_DEBUG << "Received message: " << message;
                    json message_json = json::parse(message);

                    if(message_json.find("type")==message_json.end())
                    {
                        PLOG_ERROR << "Invalid message";
                        return;
                    }
                    
                    std::string type = message_json["type"];
                    if(type == "offer")
                    {
                        if(message_json.find("description")==message_json.end())
                        {
                            PLOG_ERROR << "No sdp";
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
                                PLOG_DEBUG << "Password is correct";
                                json j;
                                j["type"] = "verify";
                                j["result"] = "success";
                                this->loginSuccessFunc_(id_card_);
                                this->ws_->send(j.dump());
                            }
                        }
                        else
                        {
                            PLOG_ERROR << "Password is incorrect";
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
                            PLOG_ERROR << "Error: No candidate field in Candidate message";
                            return;
                        }
                        if(message_json.find("mid") == message_json.end())
                        {
                            PLOG_ERROR << "Error: No mid field in Candidate message";
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
                        PLOG_ERROR << "Invalid message";
                        return;
                    }
                }
                    catch (const std::exception& e) {
        PLOG_ERROR << "WebSocket message exception: "
                  << e.what();
    }

                    PLOG_DEBUG << "Received: "
                               << message; });
    ws->onClosed([this]()
                 { 
                    close_func_(id_card_);
                    PLOG_DEBUG << "WebSocket closed"; });

    ws->onError([](std::string message)
                {
                    PLOG_ERROR << "WebSocket error: "
                            << message; });
}

void ws_server::bindCloseFunc(std::function<void(uint32_t)> cb)
{
    close_func_ = std::move(cb);
}

p2p_server::p2p_server(uint32_t id_card)
{
    PLOG_DEBUG << "p2p_server constructor, id_card: " << id_card;
    id_card_ = id_card;
}

p2p_server::~p2p_server()
{
    PLOG_DEBUG << "~p2p_server destructor";
}

void p2p_server::init(rtc::Configuration config)
{
    p2p_config_ = config;
    createPeerConnection();
}

void p2p_server::send(rtc::message_variant message)
{
    dc_->send(message);
}

void p2p_server::send(std::byte *data, size_t size)
{
    dc_->send(data, size);
}

void p2p_server::close()
{
    pc_->close();
}

void p2p_server::bindCloseFunc(std::function<void(uint32_t)> cb)
{
    close_func_ = std::move(cb);
}

void p2p_server::bindWsSend(std::function<void(std::string)> callback)
{
    send_func_ = std::move(callback);
}

void p2p_server::bindDataChannel(std::function<void(rtc::binary)> callback)
{
    data_channel_binary_callback_ = callback;
}

void p2p_server::setRemoteDescription(std::string sdp, std::string type)
{
    pc_->setRemoteDescription(rtc::Description(sdp, type));
}

void p2p_server::addRemoteCandidate(std::string candidate, std::string mid)
{
    pc_->addRemoteCandidate(rtc::Candidate(candidate, mid));
}

void p2p_server::createDataChannel(std::shared_ptr<rtc::DataChannel> dc)
{
    dc_ = dc;
    dc_->onOpen([]()
                { PLOG_DEBUG << "DataChannel opened"; });

    dc_->onClosed([]()
                  { PLOG_DEBUG << "DataChannel closed"; });

    dc_->onError([](std::string message)
                 { PLOG_ERROR << "DataChannel error: " << message; });

    dc_->onMessage([this](rtc::binary message)
                   {
        if(this->data_channel_binary_callback_)
        {
            this->data_channel_binary_callback_(message);
        }
        else
        {
            PLOG_DEBUG << "DataChannel binary message: " << message.size() << " bytes";
        } }, [this](std::string message)
                   { PLOG_DEBUG << "DataChannel message: " << message; });
}

void p2p_server::createPeerConnection()
{
    pc_ = std::make_shared<rtc::PeerConnection>(p2p_config_);

    pc_->onStateChange([this](rtc::PeerConnection::State state)
                       { 
                        if(state == rtc::PeerConnection::State::Closed)
                        {
                            close_func_(id_card_);
                        }
                        PLOG_DEBUG << "PeerConnection state: "
                                   << static_cast<int>(state); });

    pc_->onGatheringStateChange([](rtc::PeerConnection::GatheringState state)
                                { PLOG_DEBUG << "Gathering state: "
                                            << static_cast<int>(state); });

    pc_->onLocalCandidate([this](rtc::Candidate candidate)
                          { 
                            json j;
                            j["type"] = "candidate";
                            j["candidate"] = candidate;
                            j["mid"] = candidate.mid();
                            this->send_func_(j.dump());
                            //this->ws_->send(j.dump());
                            PLOG_DEBUG << "Local candidate: "
                                      << candidate; });

    pc_->onLocalDescription([this](rtc::Description description)
                            { 
                                json j;
                                j["type"]="answer";
                                j["description"]=description;
                                this->send_func_(j.dump());
                                //this->ws_->send(j.dump());
                                PLOG_DEBUG << "Local description: "
                                        << description; });

    pc_->onDataChannel([this](std::shared_ptr<rtc::DataChannel> dc)
                       {
                          PLOG_DEBUG << "New DataChannel";
                          createDataChannel(dc); });
}