#include "udp_session.h"
#include <plog/Log.h>
#include <asio.hpp>

using asio::ip::udp;
using SessionMux = p2psocks::SessionMux;
using Session = p2psocks::Session;

UdpSession::UdpSession(asio::io_context &io_context, SessionMux &mux, std::shared_ptr<Session> session, uint32_t session_id)
    : io_(io_context), mux_(mux), session_(session), recv_buf_(65535), socket_(io_context), session_id_(session_id)
{
    PLOG_DEBUG << "UdpSession created, session_id: " << session_id;
}

UdpSession::~UdpSession()
{
    PLOG_DEBUG << "UdpSession destroyed, session_id: " << session_id_;
}

void UdpSession::close()
{
    PLOG_DEBUG << "UdpSession close, session_id: " << session_id_;
    socket_.close();
}

bool UdpSession::start()
{
    asio::error_code ec;

    socket_.open(udp::v4(), ec);
    if (ec)
    {
        PLOG_ERROR
            << "open failed:"
            << ec.message()
            << ", session_id: " << session_id_;
        return false;
    }
    socket_.bind(udp::endpoint(udp::v4(), 0), ec);
    if (ec)
    {
        PLOG_ERROR
            << "bind failed:"
            << ec.message()
            << ", session_id: " << session_id_;
        return false;
    }

    PLOG_DEBUG << "UDP server listening on port " << socket_.local_endpoint().port() << ", session_id: " << session_id_;

    std::weak_ptr<UdpSession> weak_self = shared_from_this();

    session_->set_on_udp_data([weak_self](const std::string &host, uint16_t port,
                                          std::shared_ptr<std::vector<uint8_t>> data)
                              {
    if (weak_self.expired())
    {
        PLOG_WARNING << "set_on_udp_data UdpSession expired";
        return;
    }
    auto self = weak_self.lock();
    auto reply = std::make_shared<std::vector<uint8_t>>();

    reply->push_back(0x00);
    reply->push_back(0x00);
    reply->push_back(0x00);
    reply->push_back(0x01);

    asio::ip::address_v4 addr =
        asio::ip::make_address_v4(host);

    auto bytes = addr.to_bytes();

    reply->insert(reply->end(),
                  bytes.begin(),
                  bytes.end());

    reply->push_back((port >> 8) & 0xff);
    reply->push_back(port & 0xff);

    reply->insert(reply->end(), data->begin(), data->end());
    
    self->send(reply); });

    start_receive();
    return true;
}

int UdpSession::getLocalPort()
{
    return socket_.local_endpoint().port();
}

std::string UdpSession::get_local_ip()
{
    asio::ip::udp::socket sock(io_);
    sock.open(asio::ip::udp::v4());

    sock.connect(
        asio::ip::udp::endpoint(
            asio::ip::make_address("114.114.114.114"),
            53));

    return sock.local_endpoint().address().to_string();
}

void UdpSession::start_receive()
{
    auto self(shared_from_this());
    socket_.async_receive_from(
        asio::buffer(recv_buf_), remote_endpoint_,
        [self](std::error_code ec, std::size_t bytes_recvd)
        {
            if (ec)
            {
                PLOG_ERROR << "receive error: " << ec.message();
                return;
            }

            if (bytes_recvd <= 0)
            {
                PLOG_WARNING << "invalid packet bytes_recvd <= 0, stream_id:"<<self->session_->stream_id();
                return;
            }

            if (!self->client_known_)
            {
                self->client_endpoint_ = self->remote_endpoint_;
                self->client_known_ = true;
                PLOG_INFO << "first packet, record client endpoint stream_id:"<<self->session_->stream_id() << " " << self->client_endpoint_.address().to_string() << ":" << self->client_endpoint_.port();
            }
            else if (self->remote_endpoint_.address() != self->client_endpoint_.address())
            {
                PLOG_WARNING << "source ip changed, reject packet stream_id:"<<self->session_->stream_id();
                self->start_receive();
                return;
            }
            else if (self->remote_endpoint_.port() != self->client_endpoint_.port())
            {
                PLOG_INFO << "port changed, update client endpoint stream_id:"<<self->session_->stream_id();
                self->client_endpoint_ = self->remote_endpoint_;
            }

            if (self->recv_buf_[3] == 0x01)
            {
                self->send_ipv4(bytes_recvd, self->remote_endpoint_.address().to_string(), self->remote_endpoint_.port());
            }
            else if (self->recv_buf_[3] == 0x03)
            {
                self->send_domain(bytes_recvd, self->remote_endpoint_.address().to_string(), self->remote_endpoint_.port());
            }
            else
            {
                PLOG_ERROR << "invalid network type stream_id:"<<self->session_->stream_id();
            }
            self->start_receive();
        });
}

void UdpSession::send_ipv4(std::size_t bytes_recvd, const std::string &local_host, uint16_t local_port)
{
    if (bytes_recvd < 10)
    {
        PLOG_WARNING << "invalid ipv4 packet stream_id:"<<session_->stream_id();
        return;
    }
    char tmp[32];
    std::snprintf(tmp, sizeof(tmp), "%d.%d.%d.%d", recv_buf_[4], recv_buf_[5],
                  recv_buf_[6], recv_buf_[7]);
    std::string target_host = tmp;
    int target_port = (uint16_t(recv_buf_[8]) << 8) | recv_buf_[9];
    std::vector<uint8_t> data(recv_buf_.begin() + 10, recv_buf_.begin() + bytes_recvd);

    mux_.send_udp(session_->stream_id(), target_host, target_port, data);
}

void UdpSession::send_domain(std::size_t bytes_recvd, const std::string &local_host, uint16_t local_port)
{
    if (bytes_recvd < 5)
    {
        PLOG_WARNING << "invalid domain packet stream_id:"<<session_->stream_id();
        return;
    }
    int len = recv_buf_[4];
    if (5 + len + 2 > (int)bytes_recvd)
    {
        PLOG_WARNING << "invalid domain packet 2 stream_id:"<<session_->stream_id();
        return;
    }

    std::string target_host(recv_buf_.begin() + 5, recv_buf_.begin() + 5 + len);
    int target_port = (uint16_t(recv_buf_[6 + len]) << 8) | recv_buf_[7 + len];
    std::vector<uint8_t> data(recv_buf_.begin() + 8 + len, recv_buf_.begin() + bytes_recvd);

    mux_.send_udp(session_->stream_id(), target_host, target_port, data);
}

void UdpSession::send(std::shared_ptr<std::vector<uint8_t>> data)
{
    send_queue_.push_back(std::move(data));
    if (!sending_)
    {
        do_send_next();
        sending_ = true;
    }
}

void UdpSession::do_send_next()
{
    auto data = send_queue_.front();
    auto self = shared_from_this();
    socket_.async_send_to(
        asio::buffer(*data),
        client_endpoint_,
        [self](std::error_code ec, std::size_t bytes_sent)
        {
            if (ec)
            {
                PLOG_ERROR << "send error: "
                          << ec.message() << " stream_id:"<<self->session_->stream_id();
                return;
            }
            self->send_queue_.pop_front();
            if (!self->send_queue_.empty())
            {
                self->do_send_next();
            }
            else
            {
                self->sending_ = false;
            }
        });
}