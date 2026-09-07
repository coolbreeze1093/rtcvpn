#include "socks5.h"
#include <plog/Log.h>
#include <asio.hpp>

using asio::ip::tcp;
using SessionMux = p2psocks::SessionMux;

SocksServer::SocksServer(asio::io_context &io, uint16_t port, SessionMux &mux)
    : acceptor_(io, tcp::endpoint(tcp::v4(), port)), mux_(mux), io_(io), port_(port)
{
    PLOG_INFO << "SocksServer constructor";
}

SocksServer::~SocksServer()
{
    PLOG_INFO << "~SocksServer destructor";
}

void SocksServer::start()
{
    PLOG_INFO << "[本地] SOCKS5 入口已启动，监听端口 " << port_ << "\n";
    do_accept();
}

void SocksServer::stop()
{
    PLOG_INFO << "[本地] SOCKS5 入口已停止";
    acceptor_.close();
}

void SocksServer::do_accept()
{
    acceptor_.async_accept([this](std::error_code ec, tcp::socket socket)
                           {
        if (ec&&ec == asio::error::operation_aborted)
        {
            
            return;
        }
        if(ec)
        {
            PLOG_ERROR << "accept error: " << ec.message();
            do_accept();
            return;
        }

        uint32_t session_id = session_id_generator_.create_session_id();
        auto s = std::make_shared<Socks5Session>(io_, std::move(socket), mux_, session_id);
        {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            sessions_[session_id] = s;
        }
        s->set_on_close([this](uint32_t id) {
            {
                std::lock_guard<std::mutex> lock(sessions_mutex_);
                sessions_.erase(id);
            }
        });

        s->start();
        
        do_accept();
    });
}