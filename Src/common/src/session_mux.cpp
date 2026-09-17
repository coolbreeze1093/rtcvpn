#include "session_mux.h"
#include <plog/Log.h>

namespace p2psocks
{

    Session::Session(uint32_t stream_id) : stream_id_(stream_id) {}

    uint32_t Session::stream_id() const { return stream_id_; }

    void Session::set_on_data(DataCallback cb) { on_data_ = std::move(cb); }
    void Session::set_on_synack(SynAckCallback cb) { on_synack_ = std::move(cb); }
    void Session::set_on_close(CloseCallback cb) { on_close_ = std::move(cb); }
    /* void Session::set_on_udp_data(UdpCallback cb) { on_udp_data_ = std::move(cb); }
    void Session::set_on_udp_synack(UdpSynackCallback cb) { on_udp_synack_ = std::move(cb); }
    void Session::set_on_udp_close(CloseCallback cb) { on_udp_close_ = std::move(cb); } */

    SessionMux::SessionMux(uint32_t peer_conn_id)
        : peer_conn_id_(peer_conn_id) {}

    void SessionMux::set_send_func(SendFunc f) { send_func_ = std::move(f); }

    void SessionMux::set_on_syn(SynHandler h) { on_syn_ = std::move(h); }

    /* void SessionMux::set_on_udp_syn(UdpSynHandler h) { on_udp_syn_ = std::move(h); } */

    void SessionMux::on_p2p_data(uint32_t /*conn_id*/, const uint8_t *data, size_t len)
    {
        FrameHeader h;
        if (!FrameHeader::decode(data, len, h))
        {
            PLOG_WARNING << "invalid frame header stream_id " << h.stream_id;
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
                PLOG_WARNING << "invalid syn payload stream_id " << h.stream_id;
                return;
            }

            if (on_syn_)
            {
                on_syn_(h.stream_id, host, port, h.protocol);
            }
            else
            {
                PLOG_WARNING << "session not found stream_id " << h.stream_id;
            }
            break;
        }
        case FrameType::SYNACK:
        {
            if (auto s = session_manager_.find_session(h.stream_id))
            {
                bool ok = plen >= 1 && payload[0] == 0;
                if (s->on_synack_)
                {
                    s->on_synack_(ok, h.protocol);
                }
                else
                {
                    PLOG_WARNING << "session found stream_id " << h.stream_id << " on_synack_ not set";
                }
            }
            else
            {
                PLOG_WARNING << "session not found stream_id " << h.stream_id;
                return;
            }

            break;
        }
        case FrameType::DATA:
        {
            if (auto s = session_manager_.find_session(h.stream_id))
            {
                if (s->on_data_)
                {
                    s->on_data_(payload, plen, h.protocol);
                }
                else
                {
                    PLOG_WARNING << "session found stream_id " << h.stream_id << " on_data_ not set";
                    return;
                }
            }
            else
            {
                PLOG_WARNING << "session not found stream_id " << h.stream_id;
            }
            break;
        }
        case FrameType::FIN:
        {
            if (auto s = session_manager_.find_session(h.stream_id))
            {
                if (s->on_close_)
                {
                    s->on_close_(h.protocol);
                }
                else
                {
                    PLOG_WARNING << "session found stream_id " << h.stream_id << " on_close_ not set";
                    return;
                }
            }
            else
            {
                PLOG_WARNING << "session not found stream_id " << h.stream_id;
            }
            break;
        }
        /* case FrameType::UDP_SYN:
        {
            if (on_udp_syn_)
            {
                on_udp_syn_(h.stream_id);
            }
            else
            {
                PLOG_WARNING << "udp session not found stream_id " << h.stream_id;
            }
            break;
        }
        case FrameType::UDP_SYNACK:
        {
            if (auto s = session_manager_.find_session(h.stream_id))
            {
                bool ok = plen >= 1 && payload[0] == 0;
                if (s->on_udp_synack_)
                {
                    s->on_udp_synack_(ok);
                }
                else
                {
                    PLOG_WARNING << "udp session found stream_id " << h.stream_id << " on_udp_synack_ not set";
                }
            }
            else
            {
                PLOG_WARNING << "udp session not found stream_id " << h.stream_id;
                return;
            }
        }
        case FrameType::UDP_FIN:
        {
            if (auto s = session_manager_.find_session(h.stream_id))
            {
                if (s->on_udp_close_)
                {
                    s->on_udp_close_();
                }
                else
                {
                    PLOG_WARNING << "udp session found stream_id " << h.stream_id << " on_udp_close_ not set";
                }
            }
            else
            {
                PLOG_WARNING << "udp session not found stream_id " << h.stream_id;
                return;
            }
            break;
        }
        case FrameType::UDP_DATA:
        {
            if (auto s = session_manager_.find_session(h.stream_id))
            {
                if (s->on_udp_data_)
                {
                    std::string host;
                    uint16_t port;
                    std::shared_ptr<std::vector<uint8_t>> data = nullptr;
                    if (!decode_udp_payload(payload, plen, host, port, data))
                    {
                        PLOG_WARNING << "invalid udp payload stream_id " << h.stream_id;
                        return;
                    }
                    s->on_udp_data_(host, port, data);
                }
                else
                {
                    PLOG_WARNING << "udp session found stream_id " << h.stream_id << " on_udp_data_ not set";
                }
            }
            else
            {
                PLOG_WARNING << "udp session not found stream_id " << h.stream_id;
                return;
            }
            break;
        }
        case FrameType::HTTP_SYN:
        {
            std::string host;
            uint16_t port;
            if (!decode_syn_payload(payload, plen, host, port))
            {
                PLOG_WARNING << "invalid http syn payload stream_id " << h.stream_id;
                return;
            }
            if (on_http_syn_)
            {
                on_http_syn_(h.stream_id, host, port);
            }
            else
            {
                PLOG_WARNING << "http session not found stream_id " << h.stream_id;
            }
            break;
        }
        case FrameType::HTTP_SYNACK:
        {
            if (auto s = session_manager_.find_session(h.stream_id))
            {
                bool ok = plen >= 1 && payload[0] == 0;
                if (s->on_http_synack_)
                {
                    s->on_http_synack_(ok);
                }
                else
                {
                    PLOG_WARNING << "http session found stream_id " << h.stream_id << " on_http_synack_ not set";
                }
            }
            else
            {
                PLOG_WARNING << "http session not found stream_id " << h.stream_id;
                return;
            }
            break;
        }
        case FrameType::HTTP_FIN:
        {
            if(auto s = session_manager_.find_session(h.stream_id))
            {
                if (s->on_http_close_)
                {
                    s->on_http_close_();
                }
                else
                {
                    PLOG_WARNING << "http session found stream_id " << h.stream_id << " on_http_close_ not set";
                }
            }
            else
            {
                PLOG_WARNING << "http session not found stream_id " << h.stream_id;
                return;
            }
            break;
        }
        case FrameType::HTTP_DATA:
        {
            if (auto s = session_manager_.find_session(h.stream_id))
            {
                if (s->on_http_data_)
                {
                    s->on_http_data_(payload, plen);
                }
                else
                {
                    PLOG_WARNING << "http session found stream_id " << h.stream_id << " on_http_data_ not set";
                }
            }
            else
            {
                PLOG_WARNING << "http session not found stream_id " << h.stream_id;
                return;
            }
            break;
        } */
        case FrameType::DATA_CTRL:
        {
            if (auto s = session_manager_.find_session(h.stream_id))
            {
                if (s->on_data_ctrl_)
                {
                    if (plen >= 1)
                    {
                        s->on_data_ctrl_(CtrlType(payload[0]), h.protocol);
                    }
                    else
                    {
                        PLOG_WARNING << "invalid data ctrl payload stream_id " << h.stream_id;
                    }
                }
                else
                {
                    PLOG_WARNING << "session found stream_id " << h.stream_id << " on_data_ctrl_ not set";
                }
            }
            else
            {
                PLOG_WARNING << "session not found stream_id " << h.stream_id;
                return;
            }
                   }
        break;
        default:
        {
            PLOG_WARNING << "invalid frame type stream_id " << h.stream_id;
            break;
        }
        }
    }

    std::shared_ptr<Session> SessionMux::create_session()
    {
        return session_manager_.create_session();
    }

    std::shared_ptr<Session> SessionMux::create_session(uint32_t stream_id)
    {
        return session_manager_.create_session(stream_id);
    }

    void SessionMux::register_session(std::shared_ptr<Session> s)
    {
        session_manager_.register_session(s);
    }

    void SessionMux::remove_session(uint32_t stream_id)
    {
        session_manager_.remove_session(stream_id);
    }

    void SessionMux::send_syn(uint32_t stream_id, const std::string &host, uint16_t port, Protocol protocol)
    {
        auto payload = encode_syn_payload(host, port);
        auto frame = make_frame(stream_id, FrameType::SYN, protocol, payload.data(),
                                payload.size());
        if (send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_syn: send_func_ is not set";
        }
    }

    void SessionMux::send_synack(uint32_t stream_id, bool ok, Protocol protocol)
    {
        uint8_t status = ok ? 0 : 1;
        auto frame = make_frame(stream_id, FrameType::SYNACK, protocol, &status, 1);
        if (send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_synack: send_func_ is not set";
        }
    }

    void SessionMux::send_data(uint32_t stream_id, const uint8_t *data, size_t len, Protocol protocol)
    {
        auto frame = make_frame(stream_id, FrameType::DATA, protocol, data, len);
        if (send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_data: send_func_ is not set";
        }
    }

    void SessionMux::send_fin(uint32_t stream_id, Protocol protocol)
    {
        auto frame = make_frame(stream_id, FrameType::FIN, protocol);
        if (send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_fin: send_func_ is not set";
        }
    }

    /* void SessionMux::send_udp_syn(uint32_t stream_id)
    {
        auto frame = make_frame(stream_id, FrameType::UDP_SYN);
        if (send_func_)
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
        if (send_func_)
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
        if (send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_udp_fin: send_func_ is not set";
        }
    } */

    /* void SessionMux::send_udp(uint32_t stream_id, const std::string &host, uint16_t port,
                              const std::vector<uint8_t> &data)
    {
        auto payload = encode_udp_payload(host, port, data);
        auto frame = make_frame(stream_id, FrameType::DATA, payload.data(), payload.size());
        if (send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_udp: send_func_ is not set";
        }
    } */

    /* void SessionMux::send_http_syn(uint32_t stream_id, const std::string &host, uint16_t port)
    {
        auto payload = encode_syn_payload(host, port);
        auto frame = make_frame(stream_id, FrameType::HTTP_SYN, payload.data(), payload.size());
        if (send_func_)
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
        if (send_func_)
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
        if (send_func_)
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
        // PLOG_DEBUG << "send http data, stream_id=" << stream_id << ", size=" << len;
        auto frame = make_frame(stream_id, FrameType::HTTP_DATA, data, len);
        if (send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_http_data: send_func_ is not set";
        }
    } */

    void SessionMux::send_data_ctrl(uint32_t stream_id, CtrlType type, Protocol protocol)
    {
        uint8_t ctrl_type = type == CtrlType::receive ? 0 : 1;
        auto frame = make_frame(stream_id, FrameType::DATA_CTRL, protocol, &ctrl_type, 1);
        if (send_func_)
        {
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }
        else
        {
            PLOG_WARNING << "send_data_ctrl: send_func_ is not set";
        }
    }
} // namespace p2psocks