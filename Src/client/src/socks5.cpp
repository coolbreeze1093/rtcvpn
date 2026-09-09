#include "socks5.h"
#include <plog/Log.h>
#include <asio.hpp>

using asio::ip::tcp;
using SessionMux = p2psocks::SessionMux;

SocksServer::SocksServer(asio::io_context &io, SessionMux &mux)
    : acceptor_(io), mux_(mux), io_(io)
{
    PLOG_INFO << "SocksServer constructor";
}

SocksServer::~SocksServer()
{
    PLOG_INFO << "~SocksServer destructor";
}

void SocksServer::start(int16_t port)
{
    port_ = port;
    PLOG_INFO << "SocksServer start, port: " << port_ << "\n";
    acceptor_.open(tcp::v4());
    acceptor_.set_option(asio::socket_base::reuse_address(true));
    acceptor_.bind(tcp::endpoint(tcp::v4(), port_));
    acceptor_.listen();
    do_accept();
}

void SocksServer::stop()
{
    PLOG_INFO << "closing SOCKS5 server";
    if(acceptor_.is_open())
    {
        acceptor_.close();
    }
}

void SocksServer::do_accept()
{
    acceptor_.async_accept([this](std::error_code ec, tcp::socket socket)
                           {
        if (ec&&ec == asio::error::operation_aborted)
        {
            PLOG_INFO << "accept operation aborted";
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