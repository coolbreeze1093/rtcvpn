#pragma once
#include "p2p_server.h"
#include "p2p_client.h"
#include "session_mux.h"
#include <asio.hpp>

class ProcessSendData
{
public:
    ProcessSendData(std::shared_ptr<p2p_server> server, p2psocks::SessionMux &mux) : server_(server), mux_(mux)
    {
        server_->bindDataChannel(std::bind(ProcessSendData::recvBinaryData, this, std::placeholders::_1));
        mux_.set_send_func(std::bind(&ProcessSendData::sendData, this,
                                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    }

    ProcessSendData(p2p_client *client, p2psocks::SessionMux &mux) : client_(client), mux_(mux)
    {
        client_->bindDataChannel(std::bind(ProcessSendData::recvBinaryData, this, std::placeholders::_1));
        mux_.set_send_func(std::bind(&ProcessSendData::sendData, this,
                                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    }
    void sendData(uint32_t conn_id,
                  const uint8_t *data,
                  size_t len)
    {
        if (len > UINT32_MAX)
            return;

        std::vector<std::byte> buf(len + 4);

        uint32_t net_len = htonl(static_cast<uint32_t>(len));

        memcpy(buf.data(), &net_len, 4);
        memcpy(buf.data() + 4, data, len);

        if (server_)
        {
            server_->send(buf.data(), buf.size());
        }
        else if (client_)
        {
            client_->send(buf.data(), buf.size());
        }
    }

    void recvBinaryData(rtc::binary data)
    {
        if (data.size() < 4)
            return;

        uint32_t net_len;

        memcpy(&net_len, data.data(), 4);

        uint32_t len = ntohl(net_len);

        if (len > data.size() - 4)
            return;

        const uint8_t *payload =
            reinterpret_cast<const uint8_t *>(data.data()) + 4;

        mux_.on_p2p_data(
            1,
            payload,
            len);
    }

private:
    std::shared_ptr<p2p_server> server_ = nullptr;
    p2p_client *client_ = nullptr;
    p2psocks::SessionMux &mux_;
};