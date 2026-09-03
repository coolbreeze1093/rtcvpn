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
#include <plog/Log.h>

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
        using UdpCallback = std::function<void(const std::string&remote_host, uint16_t remote_port,
            std::shared_ptr<std::vector<uint8_t>> data)>;
        using UdpSynackCallback = std::function<void(bool ok)>;

        explicit Session(uint32_t stream_id);

        uint32_t stream_id() const;

        void set_on_data(DataCallback cb);
        void set_on_synack(SynAckCallback cb);
        void set_on_close(CloseCallback cb);
        void set_on_udp_data(UdpCallback cb);
        void set_on_udp_synack(UdpSynackCallback cb);
        void set_on_udp_close(CloseCallback cb);

        DataCallback on_data_;
        SynAckCallback on_synack_;
        CloseCallback on_close_;
        UdpCallback on_udp_data_;
        UdpSynackCallback on_udp_synack_;
        CloseCallback on_udp_close_;

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
        using SynHandler = std::function<void(uint32_t session_id,
                                              const std::string &host,
                                              uint16_t port)>;

        using UdpSynHandler = std::function<void(uint32_t session_id)>;

        // peer_conn_id: 你的 P2P 模块里代表"对端"的连接标识，由你在建立好P2P连接后传入
        SessionMux(uint32_t peer_conn_id);

        void set_send_func(SendFunc f);
        void set_on_syn(SynHandler h);
        void set_on_udp_syn(UdpSynHandler h);

        // ---------- 这个函数由你接到P2P模块的"收到数据"回调里调用 ----------
        // 例如: p2pModule.setOnReceive([&](uint32_t conn_id, const uint8_t* d, size_t n){
        //           mux.on_p2p_data(conn_id, d, n);
        //       });
        void on_p2p_data(uint32_t /*conn_id*/, const uint8_t *data, size_t len);

        // ---------- 会话管理 ----------
        std::shared_ptr<Session> create_session();

        std::shared_ptr<Session> create_session(uint32_t stream_id);

        void register_session(std::shared_ptr<Session> s);

        void remove_session(uint32_t stream_id);

        // ---------- 发送各类帧（内部调用你绑定的 send_func_） ----------
        void send_syn(uint32_t stream_id, const std::string &host, uint16_t port);

        void send_synack(uint32_t stream_id, bool ok);

        void send_data(uint32_t stream_id, const uint8_t *data, size_t len);

        void send_fin(uint32_t stream_id);

        void send_udp_syn(uint32_t stream_id);

        void send_udp_synack(uint32_t stream_id, bool ok);

        void send_udp_fin(uint32_t stream_id);

        void send_udp(uint32_t stream_id, const std::string&host, uint16_t port,
            const std::vector<uint8_t>& data);

    private:
        uint32_t gen_stream_id();

        SendFunc send_func_;
        uint32_t peer_conn_id_;
        SynHandler on_syn_;
        UdpSynHandler on_udp_syn_;
        std::map<uint32_t, std::shared_ptr<Session>> sessions_;
    };

} // namespace p2psocks