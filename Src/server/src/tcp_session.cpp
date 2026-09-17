#include "tcp_session.h"
#include <plog/Log.h>
#include <asio.hpp>
#include "tcp_session.h"

using asio::ip::tcp;
using namespace p2psocks;

TcpSession::TcpSession(asio::io_context &io)
    : socket_(std::make_shared<TcpSocket>(io))
{
    PLOG_DEBUG << "TcpSession created";
    

}

TcpSession::~TcpSession()
{
    PLOG_DEBUG << "TcpSession close";
}

void TcpSession::start(const std::string &host, uint16_t port)
{
    auto self = shared_from_this();
    socket_->setConnectCallback([this,self](bool success)
    {
        send_synack_(success);
    });
    socket_->setCloseCallback([this,self](){
        closeSession();
        socket_.reset();
    });

    socket_->setDataCallback([this,self](const uint8_t *d, size_t n){
        send_data_(d, n);
    });

    socket_->setWriteQueueCallback([this,self](WriteQueueStatus queue){
        send_data_ctrl(queue);
    });
    socket_->connect(host, port);
}

void TcpSession::close()
{
    socket_->close();
}

void TcpSession::revP2pData(const uint8_t *d, size_t n)
{
    socket_->send(d, n);
}

void TcpSession::start_receive()
{
    socket_->startReading();
}

void TcpSession::pause_receive()
{
    socket_->stopReading();
}
