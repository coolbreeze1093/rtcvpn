#include "session_mux.h"
#include <plog/Log.h>

namespace p2psocks
{

    Session::Session(uint32_t stream_id) : stream_id_(stream_id) {}

    uint32_t Session::stream_id() const { return stream_id_; }

    void Session::set_on_data(DataCallback cb) { on_data_ = std::move(cb); }
    void Session::set_on_synack(SynAckCallback cb) { on_synack_ = std::move(cb); }
    void Session::set_on_close(CloseCallback cb) { on_close_ = std::move(cb); }
    void Session::set_on_udp_data(UdpCallback cb) { on_udp_data_ = std::move(cb); }
    void Session::set_on_udp_synack(UdpSynackCallback cb) { on_udp_synack_ = std::move(cb); }
    void Session::set_on_udp_close(CloseCallback cb) { on_udp_close_ = std::move(cb); }

    SessionMux::SessionMux(uint32_t peer_conn_id)
        : peer_conn_id_(peer_conn_id) {}

    void SessionMux::set_send_func(SendFunc f) { send_func_ = std::move(f); }

    void SessionMux::set_on_syn(SynHandler h) { on_syn_ = std::move(h); }

    void SessionMux::set_on_udp_syn(UdpSynHandler h) { on_udp_syn_ = std::move(h); }

    void SessionMux::on_p2p_data(uint32_t /*conn_id*/, const uint8_t *data, size_t len)
    {
        FrameHeader h;
        if (!FrameHeader::decode(data, len, h))
        {
            PLOG_WARNING << "invalid frame header stream_id "<<h.stream_id;
            return;
        }

        const uint8_t *payload = data + FrameHeader::kSize;
        size_t plen = len - FrameHeader::kSize;

        switch (h.type)
        {
        case FrameType::SYN:
        {
            std::string host;
            uint16_t port;
            if (!decode_syn_payload(payload, plen, host, port))
            {
                PLOG_WARNING << "invalid syn payload stream_id "<<h.stream_id;
                return;
            }

            if (on_syn_)
            {
                on_syn_(h.stream_id, host, port);
            }
            else
            {
                PLOG_WARNING << "session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::SYNACK:
        {
            auto it = sessions_.find(h.stream_id);
            if (it == sessions_.end())
            {
                PLOG_WARNING << "session not found stream_id "<<h.stream_id;
                return;
            }
            bool ok = plen >= 1 && payload[0] == 0;
            if (it->second->on_synack_)
            {
                it->second->on_synack_(ok);
            }
            else
            {
                PLOG_WARNING << "session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::DATA:
        {
            auto it = sessions_.find(h.stream_id);
            if (it == sessions_.end())
            {
                PLOG_WARNING << "session not found stream_id "<<h.stream_id;
                return;
            }
            if (it->second->on_data_)
            {
                it->second->on_data_(payload, plen);
            }
            else
            {
                PLOG_WARNING << "session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::FIN:
        {
            auto it = sessions_.find(h.stream_id);
            if (it == sessions_.end())
            {
                PLOG_WARNING << "session not found stream_id "<<h.stream_id;
                return;
            }
            if (it->second->on_close_)
            {
                it->second->on_close_();
            }
            else
            {
                PLOG_WARNING << "session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::UDP_SYN:
        {
            if (on_udp_syn_)
            {
                on_udp_syn_(h.stream_id);
            }
            else
            {
                PLOG_WARNING << "udp session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::UDP_SYNACK:
        {
            auto it = sessions_.find(h.stream_id);
            if (it == sessions_.end())
            {
                PLOG_WARNING << "udp session not found stream_id "<<h.stream_id;
                return;
            }
            bool ok = plen >= 1 && payload[0] == 0;
            if (it->second->on_udp_synack_)
            {
                it->second->on_udp_synack_(ok);
            }
            else
            {
                PLOG_WARNING << "udp session not found stream_id "<<h.stream_id;
            }
        }
        case FrameType::UDP_FIN:
        {
            auto it = sessions_.find(h.stream_id);
            if (it == sessions_.end())
            {
                PLOG_WARNING << "udp session not found stream_id "<<h.stream_id;
                return;
            }
            if (it->second->on_udp_close_)
            {
                it->second->on_udp_close_();
            }
            else
            {
                PLOG_WARNING << "udp session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::UDP_DATA:
        {
            auto it = sessions_.find(h.stream_id);
            if (it == sessions_.end())
            {
                PLOG_WARNING << "session not found stream_id "<<h.stream_id;
                return;
            }

            std::string host;
            uint16_t port;
            std::shared_ptr<std::vector<uint8_t>> data = nullptr;
            if (!decode_udp_payload(payload, plen, host, port, data))
            {
                PLOG_WARNING << "invalid udp payload stream_id "<<h.stream_id;
                return;
            }

            if (it->second->on_udp_data_)
            {
                it->second->on_udp_data_(host, port, data);
            }
            else
            {
                PLOG_WARNING << "udp session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::HTTP_SYN:
        {
            std::string host;
            uint16_t port;
            if (!decode_syn_payload(payload, plen, host, port))
            {
                PLOG_WARNING << "invalid http syn payload stream_id "<<h.stream_id;
                return;
            }
            if (on_http_syn_)
            {
                on_http_syn_(h.stream_id, host, port);
            }
            else
            {
                PLOG_WARNING << "http session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::HTTP_SYNACK:
        {
            auto it = sessions_.find(h.stream_id);
            if (it == sessions_.end())
            {
                PLOG_WARNING << "http session not found stream_id "<<h.stream_id;
                return;
            }
            bool ok = plen >= 1 && payload[0] == 0;
            if (it->second->on_http_synack_)
            {
                it->second->on_http_synack_(ok);
            }
            else
            {
                PLOG_WARNING << "http session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::HTTP_FIN:
        {
            auto it = sessions_.find(h.stream_id);
            if (it == sessions_.end())
            {
                PLOG_WARNING << "http session not found stream_id "<<h.stream_id;
                return;
            }
            if (it->second->on_http_close_)
            {
                it->second->on_http_close_();
            }
            else
            {
                PLOG_WARNING << "http session not found stream_id "<<h.stream_id;
            }
            break;
        }
        case FrameType::HTTP_DATA:
        {
            auto it = sessions_.find(h.stream_id);
            if (it == sessions_.end())
            {
                PLOG_WARNING << "http session not found stream_id "<<h.stream_id;
                return;
            }
            if (it->second->on_http_data_)
            {
                it->second->on_http_data_(payload, plen);
            }
            else
            {
                PLOG_WARNING << "http session not found stream_id "<<h.stream_id;
            }
            break;
        }
        default:
        {
            PLOG_WARNING << "invalid frame type stream_id "<<h.stream_id;
            break;
        }
        }
    }

    std::shared_ptr<Session> SessionMux::create_session()
    {
        uint32_t sid = gen_stream_id();
        auto s = std::make_shared<Session>(sid);
        sessions_[sid] = s;
        return s;
    }

    std::shared_ptr<Session> SessionMux::create_session(uint32_t stream_id)
    {
        auto s = std::make_shared<Session>(stream_id);
        sessions_[stream_id] = s;
        return s;
    }

    void SessionMux::register_session(std::shared_ptr<Session> s)
    {
        sessions_[s->stream_id()] = s;
    }

    void SessionMux::remove_session(uint32_t stream_id) { sessions_.erase(stream_id); }

    void SessionMux::send_syn(uint32_t stream_id, const std::string &host, uint16_t port)
    {
        auto payload = encode_syn_payload(host, port);
        auto frame = make_frame(stream_id, FrameType::SYN, payload.data(),
                                payload.size());
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_syn: send_func_ is not set";
        }
    }

    void SessionMux::send_synack(uint32_t stream_id, bool ok)
    {
        uint8_t status = ok ? 0 : 1;
        auto frame = make_frame(stream_id, FrameType::SYNACK, &status, 1);
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_synack: send_func_ is not set";
        }
    }

    void SessionMux::send_data(uint32_t stream_id, const uint8_t *data, size_t len)
    {
        auto frame = make_frame(stream_id, FrameType::DATA, data, len);
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_data: send_func_ is not set";
        }
    }

    void SessionMux::send_fin(uint32_t stream_id)
    {
        auto frame = make_frame(stream_id, FrameType::FIN);
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_fin: send_func_ is not set";
        }
    }

    void SessionMux::send_udp_syn(uint32_t stream_id)
    {
        auto frame = make_frame(stream_id, FrameType::UDP_SYN);
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_udp_syn: send_func_ is not set";
        }
    }

    void SessionMux::send_udp_synack(uint32_t stream_id, bool ok)
    {
        uint8_t status = ok ? 0 : 1;
        auto frame = make_frame(stream_id, FrameType::UDP_SYNACK, &status, 1);
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_udp_synack: send_func_ is not set";
        }
    }

    void SessionMux::send_udp_fin(uint32_t stream_id)
    {
        auto frame = make_frame(stream_id, FrameType::UDP_FIN);
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_udp_fin: send_func_ is not set";
        }
    }

    void SessionMux::send_udp(uint32_t stream_id, const std::string&host, uint16_t port,
        const std::vector<uint8_t>& data)
    {
        auto payload = encode_udp_payload(host, port, data);
        auto frame = make_frame(stream_id, FrameType::UDP_DATA, payload.data(), payload.size());
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_udp: send_func_ is not set";
        }
    }

    void SessionMux::send_http_syn(uint32_t stream_id, const std::string &host, uint16_t port)
    {
        auto payload = encode_syn_payload(host, port);
        auto frame = make_frame(stream_id, FrameType::HTTP_SYN, payload.data(), payload.size());
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_http_syn: send_func_ is not set";
        }
    }

    void SessionMux::send_http_fin(uint32_t stream_id)
    {
        auto frame = make_frame(stream_id, FrameType::HTTP_FIN);
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_http_fin: send_func_ is not set";
        }
    }

    void SessionMux::send_http_synack(uint32_t stream_id, bool ok)
    {
        uint8_t status = ok ? 0 : 1;
        auto frame = make_frame(stream_id, FrameType::HTTP_SYNACK, &status, 1);
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_http_synack: send_func_ is not set";
        }
    }

    void SessionMux::send_http_data(uint32_t stream_id, const uint8_t *data, size_t len)
    {
        //PLOG_DEBUG << "send http data, stream_id=" << stream_id << ", size=" << len;
        auto frame = make_frame(stream_id, FrameType::HTTP_DATA, data, len);
        if(send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_http_data: send_func_ is not set";
        }
    }

    uint32_t SessionMux::gen_stream_id()
    {
        static std::mt19937 rng{std::random_device{}()};
        static std::uniform_int_distribution<uint32_t> dist(1, 0xFFFFFFFEu);
        uint32_t id;
        do
        {
            id = dist(rng);
        } while (sessions_.count(id));
        return id;
    }

} // namespace p2psocks