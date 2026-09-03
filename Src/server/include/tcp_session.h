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
#include "session_mux.h"
#include <array>

using asio::ip::tcp;
using namespace p2psocks;

class TcpSession : public std::enable_shared_from_this<TcpSession>
{
public:
    TcpSession(asio::io_context &io, SessionMux &mux,
              uint32_t session_id);

    ~TcpSession();

    void bind_close_func(std::function<void(uint32_t session_id)> func);

    void set_session_id(uint32_t session_id);

    void connect_target(const std::string &host, uint16_t port);

private:
    void setup_session_callbacks();

    void do_write_to_target();

    void do_read_from_target();

    void close();

    asio::io_context &io_;
    SessionMux &mux_;
    std::shared_ptr<Session> session_;
    std::function<void(uint32_t stream_id)> close_func_;
    tcp::socket target_socket_;
    std::array<uint8_t, 8192> target_buf_{};
    std::deque<std::vector<uint8_t>> to_target_queue_;
    bool is_closed_ = false;

    uint32_t session_id_ = 0;

    std::mutex mutex_;
};