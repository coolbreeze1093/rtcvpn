// session_mux.h
// 通用的"多会话复用器"，架在你已有的 P2P 模块之上：
//   - 发送：调用你的 P2P send(conn_id, data, len)
//   - 接收：你的 P2P 收到数据后，回调本类的 on_p2p_data(conn_id, data, len)
//
// 本类只负责：按 stream_id 把数据分发给对应的会话回调，
// 不做可靠性/重传/顺序保证 —— 这些如果你的 P2P 模块本身没做，
// 需要你自己在 P2P 层保证，或者告诉我，我再加一层 ARQ。
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <iostream>

#include "session_protocol.h"

namespace p2psocks
{

    class SessionMux;

    // 一条逻辑会话（对应一条浏览器 SOCKS5 连接 <-> 一条到目标的TCP连接）
    class Session
    {
    public:
        using DataCallback = std::function<void(const uint8_t *, size_t)>;
        using SynAckCallback = std::function<void(bool ok)>;
        using CloseCallback = std::function<void()>;

        explicit Session(uint32_t stream_id) : stream_id_(stream_id) {}

        uint32_t stream_id() const { return stream_id_; }

        void set_on_data(DataCallback cb) { on_data_ = std::move(cb); }
        void set_on_synack(SynAckCallback cb) { on_synack_ = std::move(cb); }
        void set_on_close(CloseCallback cb) { on_close_ = std::move(cb); }

        DataCallback on_data_;
        SynAckCallback on_synack_;
        CloseCallback on_close_;

    private:
        uint32_t stream_id_;
    };

    class SessionMux
    {
    public:
        // SendFunc: 绑定你的 P2P 模块的发送函数，签名对应 send(conn_id, data, len)
        using SendFunc =
            std::function<void(uint32_t conn_id, const uint8_t *data, size_t len)>;

        // SynHandler: 远端角色用，收到新会话请求(host,port)时触发
        using SynHandler = std::function<void(std::shared_ptr<Session> session,
                                              const std::string &host,
                                              uint16_t port)>;

        // peer_conn_id: 你的 P2P 模块里代表"对端"的连接标识，由你在建立好P2P连接后传入
        SessionMux(uint32_t peer_conn_id)
            : peer_conn_id_(peer_conn_id) {}

        void set_send_func(SendFunc f) { send_func_ = std::move(f); }

        void set_on_syn(SynHandler h) { on_syn_ = std::move(h); }

        // ---------- 这个函数由你接到P2P模块的"收到数据"回调里调用 ----------
        // 例如: p2pModule.setOnReceive([&](uint32_t conn_id, const uint8_t* d, size_t n){
        //           mux.on_p2p_data(conn_id, d, n);
        //       });
        void on_p2p_data(uint32_t /*conn_id*/, const uint8_t *data, size_t len)
        {
            FrameHeader h;
            if (!FrameHeader::decode(data, len, h))
            {
                std::cout << "invalid frame header" << std::endl;
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
                    return;
                auto s = std::make_shared<Session>(h.stream_id);
                sessions_[h.stream_id] = s;
                if (on_syn_)
                    on_syn_(s, host, port);
                break;
            }
            case FrameType::SYNACK:
            {
                auto it = sessions_.find(h.stream_id);
                if (it == sessions_.end())
                    return;
                bool ok = plen >= 1 && payload[0] == 0;
                if (it->second->on_synack_)
                    it->second->on_synack_(ok);
                break;
            }
            case FrameType::DATA:
            {
                auto it = sessions_.find(h.stream_id);
                if (it == sessions_.end())
                    return;
                if (it->second->on_data_)
                    it->second->on_data_(payload, plen);
                break;
            }
            case FrameType::FIN:
            {
                auto it = sessions_.find(h.stream_id);
                if (it == sessions_.end())
                    return;
                if (it->second->on_close_)
                    it->second->on_close_();
                sessions_.erase(it);
                break;
            }
            case FrameType::RST:
            {
                auto it = sessions_.find(h.stream_id);
                if (it == sessions_.end())
                    return;

                // 异常关闭
                if (it->second->on_close_)
                    it->second->on_close_();

                sessions_.erase(it);

                break;
            }
            default:
            {
                std::cout << "invalid frame type" << std::endl;
                break;
            }
            }
        }

        // ---------- 会话管理 ----------
        std::shared_ptr<Session> create_session()
        {
            uint32_t sid = gen_stream_id();
            auto s = std::make_shared<Session>(sid);
            sessions_[sid] = s;
            return s;
        }

        void register_session(std::shared_ptr<Session> s)
        {
            sessions_[s->stream_id()] = s;
        }

        void remove_session(uint32_t stream_id) { sessions_.erase(stream_id); }

        // ---------- 发送各类帧（内部调用你绑定的 send_func_） ----------
        void send_syn(uint32_t stream_id, const std::string &host, uint16_t port)
        {
            auto payload = encode_syn_payload(host, port);
            auto frame = make_frame(stream_id, FrameType::SYN, payload.data(),
                                    payload.size());
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }

        void send_synack(uint32_t stream_id, bool ok)
        {
            uint8_t status = ok ? 0 : 1;
            auto frame = make_frame(stream_id, FrameType::SYNACK, &status, 1);
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }

        void send_data(uint32_t stream_id, const uint8_t *data, size_t len)
        {
            auto frame = make_frame(stream_id, FrameType::DATA, data, len);
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }

        void send_fin(uint32_t stream_id)
        {
            auto frame = make_frame(stream_id, FrameType::FIN);
            send_func_(peer_conn_id_, frame.data(), frame.size());
        }

    private:
        uint32_t gen_stream_id()
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

        SendFunc send_func_;
        uint32_t peer_conn_id_;
        SynHandler on_syn_;
        std::map<uint32_t, std::shared_ptr<Session>> sessions_;
    };

} // namespace p2psocks
