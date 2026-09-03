#pragma once
#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include "session_mux.h"
#include <array>

using asio::ip::udp;
using SessionMux = p2psocks::SessionMux;
using Session = p2psocks::Session;


class UdpClient : public std::enable_shared_from_this<UdpClient>
{
public:
    UdpClient(asio::io_context &io_context, SessionMux &mux, uint32_t session_id);

    ~UdpClient();

    bool start();

    void bind_close_func(std::function<void(uint32_t session_id)> close_func);

    void set_session_id(int32_t session_id);

private:
    void send(std::shared_ptr<std::vector<uint8_t>> data, const std::string &target_host, int target_port);

    void do_send_next();

    void start_receive();

    void close();

    asio::io_context &io_;
    udp::socket socket_;
    udp::endpoint server_endpoint_;
    udp::endpoint sender_endpoint_;
    SessionMux &mux_;
    std::vector<uint8_t> recv_buf_;
    std::shared_ptr<Session> session_;

    std::deque<p2psocks::SendData> send_queue_;
    std::function<void(uint32_t session_id)> close_func_;
    int32_t session_id_{0};

    bool sending_{false};
    std::mutex mutex_;
    bool is_closed_{false};
};