// local.cpp —— 本地端 (Ingress)
// 职责：接收浏览器 SOCKS5 连接 -> 解析目标host:port -> 通过 SessionMux
//       (最终调用你的P2P模块 send()) 把请求发给远端 -> 远端连接成功后
//       双向转发数据。
//
// !!! 需要你接入的地方，我都用 "TODO(P2P)" 标出了 !!!
//
// 编译:
//   g++ -std=c++17 -O2 -DASIO_STANDALONE local.cpp -lpthread -o local
#pragma once
#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include "session_mux.h"
#include "session_id_generator.h"
#include "tcp_session.h"

using asio::ip::tcp;
using SessionMux = p2psocks::SessionMux;

class SocksServer
{
public:
    SocksServer(asio::io_context &io, uint16_t port, SessionMux &mux)
        : acceptor_(io, tcp::endpoint(tcp::v4(), port)), mux_(mux), io_(io)
    {
        PLOG_INFO << "[本地] SOCKS5 入口已启动，监听端口 " << port << "\n";
        do_accept();
    }

private:
    void do_accept()
    {
        acceptor_.async_accept([this](std::error_code ec, tcp::socket socket)
                               {
            if (!ec) {
                uint32_t session_id = session_id_generator_.create_session_id();
                auto s = std::make_shared<Socks5Session>(io_, std::move(socket), mux_, session_id);
                sessions_[session_id] = s;
                s->set_on_close([this](uint32_t id) {
                    sessions_.erase(id);
                });
                s->start();
            }
            do_accept(); });
    }
    tcp::acceptor acceptor_;
    SessionMux &mux_;
    asio::io_context &io_;
    std::unordered_map<uint32_t, std::shared_ptr<Socks5Session>> sessions_;
    SessionIdGenerator session_id_generator_;
};