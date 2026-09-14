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
    UdpClient(asio::io_context &io_context, std::weak_ptr<SessionMux> weak_mux, uint32_t stream_id);

    ~UdpClient();

    bool start();

    void bind_close_func(std::function<void(uint32_t session_id)> close_func);

    void close();

private:
    void send(std::shared_ptr<std::vector<uint8_t>> data, const std::string &target_host, int target_port);

    void do_send_next();

    void do_read_from_target();

    void close_func();

    void start_receive(){if(!is_receiving_){is_receiving_ = true;do_read_from_target();}}
    void pause_receive(){is_receiving_ = false;};

    bool is_receiving_ = true;

    asio::io_context &io_;
    udp::socket socket_;
    udp::endpoint server_endpoint_;
    udp::endpoint sender_endpoint_;
    std::weak_ptr<SessionMux> weak_mux_;
    std::vector<uint8_t> recv_buf_;
    std::shared_ptr<Session> session_;

    std::deque<p2psocks::SendData> send_queue_;
    std::function<void(uint32_t session_id)> close_func_;
    int32_t stream_id_{0};

    bool sending_{false};
    std::mutex mutex_;
    bool is_closed_{false};
};