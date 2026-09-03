#include "process_new_client.h"
#include <plog/Log.h>

Socks5Session::Socks5Session(asio::io_context &io,
              rtc::Configuration &config,
              uint32_t session_id)
    : io_(io), config_(config), session_id_(session_id),
      mux_(session_id_) // 每个 Socks5Session 独立的 mux，peer_conn_id 用 session_id 即可
{
    PLOG_DEBUG << "Socks5Session created  " << session_id;
}

Socks5Session::~Socks5Session()
{
    PLOG_DEBUG << "Socks5Session destroyed  " << session_id_;
}

void Socks5Session::start(std::shared_ptr<rtc::WebSocket> ws)
{
    ws_ = std::make_shared<ws_server>(session_id_);

    ws_->bindLoginSuccess(std::bind(&Socks5Session::onLoginSuccess, shared_from_this()));

    ws_->bindCloseFunc([this](uint32_t session_id)
    {
        PLOG_INFO << "websocket closed  " << session_id;
        notifyClose();
    });

    ws_->connect(ws);
}

void Socks5Session::bindCloseFunc(std::function<void(uint32_t)> cb)
{
    close_cb_ = std::move(cb);
}

uint32_t Socks5Session::id() const { return session_id_; }

void Socks5Session::onLoginSuccess()
{
    p2p_ = std::make_shared<p2p_server>(session_id_);
    p2p_->init(config_);

    p2p_->bindWsSend(std::bind(&ws_server::send, ws_, std::placeholders::_1));

    p2p_->bindDataChannel([this](rtc::binary data)
                             {
                    auto result = p2psocks::unpackMessage(data.data(), data.size());
                    if (!result)
                    {
                        return;
                    }

                mux_.on_p2p_data(1, result->payload, result->len); 
            });

    p2p_->bindCloseFunc([this](uint32_t session_id)
    {
        PLOG_INFO << "p2p closed  " << session_id;
        p2p_.reset();
        tcp_sessions_.clear(); // mux 没了，底下挂的转发也该一并清理
        udp_sessions_.clear();

        // 通知 ws 关闭
    });

    ws_->bindsetRemoteDescriptionFunc(
        std::bind(&p2p_server::setRemoteDescription, p2p_,
                  std::placeholders::_1, std::placeholders::_2));

    ws_->bindaddRemoteCandidateFunc(
        std::bind(&p2p_server::addRemoteCandidate, p2p_,
                  std::placeholders::_1, std::placeholders::_2));

    
    mux_.set_send_func([this](uint32_t conn_id, const uint8_t *data, size_t len)
                       {
        try
        {
            std::vector<std::byte> buf = p2psocks::packMessage(data, len);
            p2p_->send(buf.data(), buf.size());
        }
        catch (const std::length_error &e)
        {
            PLOG_ERROR << "sendData: " << e.what();
            return;
        }
    });

    // 每个连接自己的 SessionMux，注册自己的 on_syn / on_udp_syn
    mux_.set_on_syn([this](uint32_t stream_id,
                            const std::string &host, uint16_t port)
    {
        PLOG_DEBUG << "rev tcp syn " << host << ":" << port
                  << " (stream_id=" << stream_id << ", session=" << session_id_ << ")\n";
        auto rs = std::make_shared<TcpSession>(io_, mux_, stream_id);
        tcp_sessions_[stream_id] = rs;
        rs->bind_close_func([this](uint32_t stream_id)
        {
            tcp_sessions_.erase(stream_id);
        });
        rs->connect_target(host, port);
    });

    mux_.set_on_udp_syn([this](uint32_t stream_id)
    {
        PLOG_DEBUG << "rev udp syn (stream_id=" << stream_id
                  << ", session=" << session_id_ << ")\n";
        auto udp = std::make_shared<UdpClient>(io_, mux_, stream_id);
        udp_sessions_[stream_id] = udp;
        udp->bind_close_func([this](uint32_t stream_id)
        {
            udp_sessions_.erase(stream_id);
        });
        udp->start();
    });

    
}

void Socks5Session::notifyClose()
{
    p2p_->close();
    if (close_cb_) close_cb_(session_id_);
}

ProcessNewWsClient::ProcessNewWsClient(asio::io_context &io, rtc::Configuration config)
    : io_(io), config_(config)
{
    PLOG_DEBUG << "ProcessNewWsClient created";
}

ProcessNewWsClient::~ProcessNewWsClient()
{
    PLOG_DEBUG << "ProcessNewWsClient destroyed";
}

void ProcessNewWsClient::newClient(std::shared_ptr<rtc::WebSocket> ws)
{
    uint32_t id = create_session_id();

    auto session = std::make_shared<Socks5Session>(io_, config_, id);
    session->bindCloseFunc([this](uint32_t session_id)
    {
        client_sessions_.erase(session_id);
    });

    client_sessions_[id] = session;
    session->start(ws);

    PLOG_DEBUG << "newClient Socks5Session started  " << id;
}

uint32_t ProcessNewWsClient::create_session_id()
{
    if (id_ > 65535) id_ = 0;
    return ++id_;
}