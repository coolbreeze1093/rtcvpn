#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include "session_mux.h"
using asio::ip::udp;
using SessionMux = p2psocks::SessionMux;
using Session = p2psocks::Session;

class UdpSession : public std::enable_shared_from_this<UdpSession>
{

public:
    UdpSession(asio::io_context &io_context, SessionMux &mux, std::shared_ptr<Session> session, uint32_t session_id);

    ~UdpSession();

    void close();

    bool start();

    int getLocalPort();

    std::string get_local_ip();

private:
    void start_receive();

    void send_ipv4(std::size_t bytes_recvd, const std::string &local_host, uint16_t local_port);

    void send_domain(std::size_t bytes_recvd, const std::string &local_host, uint16_t local_port);
    // 发送回客户端
    void send(std::shared_ptr<std::vector<uint8_t>> data);

    void do_send_next();

    asio::io_context &io_;
    SessionMux &mux_;
    std::shared_ptr<Session> session_;
    std::vector<uint8_t> recv_buf_;
    udp::socket socket_;
    udp::endpoint remote_endpoint_;
    udp::endpoint client_endpoint_;
    std::deque<std::shared_ptr<std::vector<uint8_t>>> send_queue_;
    bool sending_ = false;
    bool client_known_ = false;
    uint32_t session_id_;
};