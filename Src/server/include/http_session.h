// remote.cpp —— 远端 (Egress)
// 职责：收到 P2P 隧道传来的 SYN(host,port) -> 真正 connect() 目标服务器
//       -> 把连接结果通过 SYNACK 回给本地端 -> 之后双向转发数据。
//
// !!! 需要你接入的地方，我都用 "TODO(P2P)" 标出了 !!!
//
// 编译:
//   g++ -std=c++17 -O2 -DASIO_STANDALONE remote.cpp -lpthread -o remote
#pragma once
#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include <array>
#include "http_parse.h"
#include "session_interface.h"
#include "tcp_socket.h"

using asio::ip::tcp;
using namespace p2psocks;

class HttpSession : public std::enable_shared_from_this<HttpSession>, public SessionInterface
{
public:
    HttpSession(asio::io_context &io);

    ~HttpSession();

    void start(const std::string &host, uint16_t port) override;

    void close() override;

    void revP2pData(const uint8_t *d, size_t n) override;

    void start_receive() override;

    void pause_receive() override;

private:
    std::shared_ptr<TcpSocket> target_socket_;
    HttpParser http_parse_request_;
};