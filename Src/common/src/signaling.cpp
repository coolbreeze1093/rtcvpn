#include "signaling.h"
#include <plog/Log.h>

void SignalingClient::connect(const std::string &url,const rtc::WebSocketConfiguration &config)
{
    ws_ = std::make_shared<rtc::WebSocket>(config);
    bindWebSocket();
    ws_->open(url);
}

void SignalingClient::connect(std::shared_ptr<rtc::WebSocket> ws)
{
    ws_ = std::move(ws);
    bindWebSocket();
}

void SignalingClient::disconnect()
{
    PLOG_DEBUG << "SignalingClient disconnect";
    if (ws_)
        ws_->close();
}

void SignalingClient::send(const json &message)
{

    if (ws_ && ws_->isOpen())
    {
        PLOG_DEBUG << "send: " << message.dump();
        ws_->send(message.dump());
    }
    else
    {
        PLOG_ERROR << "send: WebSocket is not open";
    }
}

void SignalingClient::bindWebSocket()
{
    ws_->onOpen([this]()
    {
        PLOG_INFO << "WebSocket opened";
        if (open_callback_)
            open_callback_();
    });

    ws_->onMessage([](rtc::binary) {},
        [this](std::string message)
        {
            PLOG_INFO << "Received: " << message;
            json message_json;
            try
            {
                message_json = json::parse(message);
            }
            catch (const std::exception &e)
            {
                PLOG_ERROR << "Invalid json: " << e.what();
                return;
            }

            if (message_json.find("type") == message_json.end())
            {
                PLOG_ERROR << "Invalid message: no type field";
                return;
            }

            if (message_callback_)
                message_callback_(message_json);
        });

    ws_->onClosed([this]()
    {
        PLOG_INFO << "WebSocket closed";
        if (closed_callback_)
            closed_callback_();
    });

    ws_->onError([this](std::string message)
    {
        PLOG_ERROR << "WebSocket error: " << message;
        if (error_callback_)
            error_callback_(message);
    });
}