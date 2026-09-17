#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include "session_mux.h"
#include "udp_socket.h"
class UdpSession : public std::enable_shared_from_this<UdpSession>
{
    using SessionMux = p2psocks::SessionMux;
    using Session = p2psocks::Session;
    using UdpSocket = p2psocks::UdpSocket;
public:
    UdpSession(asio::io_context &io_context, SessionMux &mux, uint32_t session_id);

    ~UdpSession();

    void close();

    bool start();

    void revP2pData(const uint8_t *d, size_t n);

    int getLocalPort();

    std::string get_local_ip();
    void p2p_data_ctrl(p2psocks::CtrlType ctrl);

private:
    void send_ipv4(const uint8_t *d, size_t n);

    void send_domain(const uint8_t *d, size_t n);

    void send_p2p_data(const uint8_t *d, size_t n);

    asio::io_context &io_;
    SessionMux &mux_;
    std::shared_ptr<UdpSocket> socket_;
    bool client_known_ = false;
    std::string remote_ip_;
    uint16_t remote_port_;
    uint32_t session_id_;
    p2psocks::Protocol protocol_ = p2psocks::Protocol::UdpAssociate;
};