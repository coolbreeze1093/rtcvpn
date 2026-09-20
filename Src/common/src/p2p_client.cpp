#include "p2p_client.h"
#include <plog/Log.h>

void P2PClient::init(rtc::Configuration config)
{
    p2p_config_ = config;
}

void P2PClient::createDataChannel(const std::string & label)
{
    createDataChannelPrivate(label);
}

void P2PClient::handleSignalMessage(const json &message_json)
{
    std::string type = message_json.value("type", "");

    if (type == "answer" || type == "offer")
    {
        if (message_json.find("description") == message_json.end())
        {
            PLOG_ERROR << "No sdp";
            return;
        }
        std::string sdp = message_json["description"];
        if (pc_)
            pc_->setRemoteDescription(rtc::Description(sdp, type));
    }
    else if (type == "candidate")
    {
        if (message_json.find("candidate") == message_json.end())
        {
            PLOG_ERROR << "Error: No candidate field in Candidate message";
            return;
        }
        if (message_json.find("mid") == message_json.end())
        {
            PLOG_ERROR << "Error: No mid field in Candidate message";
            return;
        }

        std::string candidate = message_json["candidate"];
        std::string mid = message_json["mid"];
        if (pc_)
            pc_->addRemoteCandidate(rtc::Candidate(candidate, mid));
    }
    else
    {
        PLOG_ERROR << "P2PClient: unhandled message type: " << type;
    }
}

void P2PClient::send(rtc::message_variant message)
{
    try
    {
        if (dc_ && dc_->isOpen())
            dc_->send(message);
    }
    catch (const std::exception &e)
    {
        PLOG_ERROR << "P2PClient: send message: " << e.what();
    }
}

void P2PClient::send(const std::byte *data, size_t size)
{
    try
    {
        if (dc_ && dc_->isOpen())
            dc_->send(data, size);
    }
    catch (const std::exception &e)
    {
        PLOG_ERROR << "P2PClient: send data: " << e.what();
    }
}

void P2PClient::close()
{
    try
    {
        if (dc_ && dc_->isOpen())
            dc_->close();

        if (pc_ && pc_->state() == rtc::PeerConnection::State::Connected)
            pc_->close();
    }
    catch (const std::exception &e)
    {
        PLOG_ERROR << "P2PClient: close: " << e.what();
    }
}

void P2PClient::bindDataChannel()
{
    dc_->onOpen([this]()
                { PLOG_DEBUG << "DataChannel opened" << "max message size: " << dc_->maxMessageSize(); });

    dc_->onClosed([]()
                  { PLOG_INFO << "DataChannel closed"; });

    dc_->onError([](std::string message)
                 { PLOG_ERROR << "DataChannel error: " << message; });

    dc_->onMessage(
        [this](rtc::binary message)
        {
            if (data_channel_binary_callback_)
                data_channel_binary_callback_(message);
            else
                PLOG_INFO << "rev Data data_channel_binary_callback_ is null";
        },
        [](std::string message)
        { PLOG_INFO << "DataChannel message: " << message; });
}

void P2PClient::createDataChannelPrivate(const std::string & label)
{
    try
    {
        dc_ = pc_->createDataChannel(label);
        bindDataChannel();
    }
    catch (const std::exception &e)
    {
        PLOG_ERROR << "P2PClient: createDataChannel: " << e.what();
    }
}

void P2PClient::createDataChannelPrivate(std::shared_ptr<rtc::DataChannel> dc)
{
    dc_ = dc;
    bindDataChannel();
}

void P2PClient::createPeerConnection()
{
    try
    {
        pc_ = std::make_shared<rtc::PeerConnection>(p2p_config_);

        pc_->onStateChange([this](rtc::PeerConnection::State state)
                           {
        PLOG_INFO << "PeerConnection state: " << static_cast<int>(state);
        if (state_change_callback_)
            state_change_callback_(state); });

        pc_->onGatheringStateChange([](rtc::PeerConnection::GatheringState state)
                                    { PLOG_INFO << "Gathering state: " << static_cast<int>(state); });

        pc_->onLocalCandidate([this](rtc::Candidate candidate)
                              {
        json j;
        j["type"] = "candidate";
        j["candidate"] = candidate;
        j["mid"] = candidate.mid();
        PLOG_INFO << "Local candidate: " << candidate;
        if (signal_out_callback_)
            signal_out_callback_(j); });

        pc_->onLocalDescription([this](rtc::Description description)
                                {
        json j;
        j["type"] = "offer";
        j["description"] = description;
        PLOG_INFO << "Local description: " << description;
        if (signal_out_callback_)
            signal_out_callback_(j); });

        pc_->onDataChannel([this](std::shared_ptr<rtc::DataChannel> data_channel)
                           {
        PLOG_INFO << "DataChannel opened" << "max message size: " << data_channel->maxMessageSize();
        createDataChannelPrivate(data_channel); });
    }
    catch (const std::exception &e)
    {
        PLOG_ERROR << "P2PClient: createPeerConnection: " << e.what();
    }
}