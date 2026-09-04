// session_protocol.h
// 一条 P2P 连接(conn_id 由你的P2P模块管理) 上会跑很多条"逻辑会话"
// (对应浏览器发起的每一条 SOCKS5 连接)。这里定义会话帧格式，
// 用 stream_id 区分不同的浏览器连接。
//
// 帧格式（这就是你调用 send(conn_id, data) 时的 data 内容）：
//   [stream_id: 4B][type: 1B][payload...]
#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <asio.hpp>
#include <plog/Log.h>


namespace p2psocks
{

    enum class FrameType : uint8_t
    {
        SYN = 0x01,        // 请求建立到目标的连接, payload = host_len(1)+host+port(2)
        SYNACK = 0x02,     // 连接结果, payload = 1字节状态(0=成功,1=失败)
        DATA = 0x03,       // 数据, payload = 原始字节
        FIN = 0x04,        // 关闭该会话, payload 为空
        UDP_SYN = 0x05,    // UDP 连接请求, payload = host_len(1)+host+port(2)
        UDP_SYNACK = 0x06, // UDP 连接结果, payload = 1字节状态(0=成功,1=失败)
        UDP_FIN = 0x07,    // UDP 关闭该会话, payload 为空
        UDP_DATA = 0x08,   // UDP 数据, payload = 原始字节
    };

    struct SendData
    {
        std::string target_host;
        int target_port;
        std::shared_ptr<std::vector<uint8_t>> data;
    };

    struct FrameHeader
    {
        uint32_t stream_id;
        FrameType type;

        static constexpr size_t kSize = 5;

        void encode(uint8_t *out) const;

        static bool decode(const uint8_t *data, size_t len, FrameHeader &h);

        static uint32_t hton32(uint32_t v);
    };

    // 拼一个完整帧: header + payload
    std::vector<uint8_t> make_frame(uint32_t stream_id, FrameType type,
                                           const uint8_t *payload = nullptr,
                                           size_t payload_len = 0);

    std::vector<uint8_t> encode_syn_payload(const std::string &host,
                                                   uint16_t port);

    bool decode_syn_payload(const uint8_t *data, size_t len,
                                   std::string &host, uint16_t &port);

    // ------------------------------------------------------------
    // 打包: host, port, data
    // 格式:
    //   [1B  host_len][host bytes]
    //   [2B  port (大端)]
    //   [4B  data_len (大端)][data bytes]
    // ------------------------------------------------------------
    std::vector<uint8_t> encode_udp_payload(
        const std::string &host, uint16_t port,
        const std::vector<uint8_t> &data);

    // ------------------------------------------------------------
    // 解包
    // ------------------------------------------------------------
    bool decode_udp_payload(
        const uint8_t *buf,
        size_t len,
        std::string &host,
        uint16_t &port,
        std::shared_ptr<std::vector<uint8_t>>& data);

    std::vector<std::byte> packMessage(const uint8_t *data, size_t len);

    // ---------- 解包结果 ----------
    struct UnpackedMessage
    {
        const uint8_t *payload;
        uint32_t len;
    };

    // ---------- 解包：从原始字节中取出 len + payload ----------
    // 返回 std::nullopt 表示数据不合法
    std::optional<UnpackedMessage> unpackMessage(const std::byte *data, size_t size);

} // namespace p2psocks