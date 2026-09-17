#include "udp_session.h"
#include <plog/Log.h>
#include <asio.hpp>
#include "udp_session.h"

using SendData = p2psocks::SendData;

UdpClient::UdpClient(asio::io_context &io_context)
    : socket_(std::make_shared<p2psocks::UdpSocket>(io_context))
{
    PLOG_DEBUG << "UdpClient created";
    
    
}

UdpClient::~UdpClient()
{
    PLOG_DEBUG << "UdpClient destroyed";
}

void UdpClient::start(const std::string &server_host, uint16_t server_port)
{
    auto self = shared_from_this();
    socket_->setCloseCallback([this,self](){
        closeSession();
        socket_.reset();
    });
    socket_->setWriteQueueCallback([this,self](p2psocks::WriteQueueStatus queue) {
        send_data_ctrl(queue);
    });
    socket_->setDataCallback([this,self](const std::string &remote_host,
                                             uint16_t remote_port,
                                             const uint8_t *data, size_t len) {
        std::vector<uint8_t> payload = p2psocks::encode_udp_payload(remote_host, remote_port,data, len);
        send_data_(payload.data(), payload.size());
    });
    socket_->setOpenCallback([this,self](bool open) {
        send_synack_(open);
    });
    socket_->start();
}

void UdpClient::close()
{
    socket_->close();
}

void UdpClient::revP2pData(const uint8_t *d, size_t n)
{
    std::string remote_host;
    uint16_t remote_port;
    std::shared_ptr<std::vector<uint8_t>> data;
    
    p2psocks::decode_udp_payload(d, n, remote_host, remote_port, data);
    if (data)
    {
        socket_->send(data, remote_host, remote_port);
    }
}

void UdpClient::start_receive()
{
    socket_->startReading();
}

void UdpClient::pause_receive()
{
    socket_->stopReading();
}