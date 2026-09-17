#pragma once
#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include "session_mux.h"
#include <array>
#include "session_interface.h"
#include "udp_socket.h"



class UdpClient : public std::enable_shared_from_this<UdpClient>, public SessionInterface
{
    using Session = p2psocks::UdpSocket;
public:
    UdpClient(asio::io_context &io_context);

    ~UdpClient();

    void start(const std::string &server_host, uint16_t server_port) override;

    void close() override;

    void revP2pData(const uint8_t *d, size_t n) override;

    void start_receive();

    void pause_receive();

private:

    std::shared_ptr<p2psocks::UdpSocket> socket_;
};