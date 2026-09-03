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

        void encode(uint8_t *out) const
        {
            uint32_t sid = hton32(stream_id);
            std::memcpy(out, &sid, 4);
            out[4] = static_cast<uint8_t>(type);
        }

        static bool decode(const uint8_t *data, size_t len, FrameHeader &h)
        {
            if (len < kSize)
                return false;
            uint32_t sid;
            std::memcpy(&sid, data, 4);
            h.stream_id = hton32(sid);
            h.type = static_cast<FrameType>(data[4]);
            return true;
        }

        static uint32_t hton32(uint32_t v)
        {
            return ((v & 0x000000FFu) << 24) | ((v & 0x0000FF00u) << 8) |
                   ((v & 0x00FF0000u) >> 8) | ((v & 0xFF000000u) >> 24);
        }
    };

    // 拼一个完整帧: header + payload
    inline std::vector<uint8_t> make_frame(uint32_t stream_id, FrameType type,
                                           const uint8_t *payload = nullptr,
                                           size_t payload_len = 0)
    {
        std::vector<uint8_t> buf(FrameHeader::kSize + payload_len);
        FrameHeader h{stream_id, type};
        h.encode(buf.data());
        if (payload_len)
            std::memcpy(buf.data() + FrameHeader::kSize, payload, payload_len);
        return buf;
    }

    inline std::vector<uint8_t> encode_syn_payload(const std::string &host,
                                                   uint16_t port)
    {
        std::vector<uint8_t> out;
        out.push_back(static_cast<uint8_t>(host.size()));
        out.insert(out.end(), host.begin(), host.end());
        out.push_back(static_cast<uint8_t>(port >> 8));
        out.push_back(static_cast<uint8_t>(port & 0xFF));
        return out;
    }

    inline bool decode_syn_payload(const uint8_t *data, size_t len,
                                   std::string &host, uint16_t &port)
    {
        if (len < 1)
            return false;
        uint8_t hlen = data[0];
        if (len < 1u + hlen + 2u)
            return false;
        host.assign(reinterpret_cast<const char *>(data + 1), hlen);
        port = (static_cast<uint16_t>(data[1 + hlen]) << 8) | data[2 + hlen];
        return true;
    }

    // ------------------------------------------------------------
    // 打包: host, port, data
    // 格式:
    //   [1B  host_len][host bytes]
    //   [2B  port (大端)]
    //   [4B  data_len (大端)][data bytes]
    // ------------------------------------------------------------
    inline std::vector<uint8_t> encode_udp_payload(
        const std::string &host, uint16_t port,
        const std::vector<uint8_t> &data)
    {

        std::vector<uint8_t> out;
        out.reserve(1 + host.size() + 2 +
                    4 + data.size());

        // host
        out.push_back(static_cast<uint8_t>(host.size()));
        out.insert(out.end(), host.begin(), host.end());

        // port
        out.push_back(static_cast<uint8_t>(port >> 8));
        out.push_back(static_cast<uint8_t>(port & 0xFF));

        // data_len (4B 大端)
        uint32_t dlen = static_cast<uint32_t>(data.size());
        out.push_back(static_cast<uint8_t>((dlen >> 24) & 0xFF));
        out.push_back(static_cast<uint8_t>((dlen >> 16) & 0xFF));
        out.push_back(static_cast<uint8_t>((dlen >> 8) & 0xFF));
        out.push_back(static_cast<uint8_t>(dlen & 0xFF));

        // data
        out.insert(out.end(), data.begin(), data.end());

        return out;
    }

    // ------------------------------------------------------------
    // 解包
    // ------------------------------------------------------------
    inline bool decode_udp_payload(
        const uint8_t *buf,
        size_t len,
        std::string &host,
        uint16_t &port,
        std::shared_ptr<std::vector<uint8_t>> data)
    {

        if (!buf || len < 1)
        {
            return false;
        }

        size_t pos = 0;

        const size_t host_len = buf[pos++];

        if (len - pos < host_len + 2 + 4)
        {
            return false;
        }

        host.assign(reinterpret_cast<const char *>(buf + pos), host_len);
        pos += host_len;

        port = static_cast<uint16_t>(buf[pos]) << 8 |
               static_cast<uint16_t>(buf[pos + 1]);
        pos += 2;

        const uint32_t data_len =
            (static_cast<uint32_t>(buf[pos]) << 24) |
            (static_cast<uint32_t>(buf[pos + 1]) << 16) |
            (static_cast<uint32_t>(buf[pos + 2]) << 8) |
            static_cast<uint32_t>(buf[pos + 3]);
        pos += 4;

        if (static_cast<size_t>(data_len) > len - pos)
        {
            return false;
        }

        data->assign(buf + pos, buf + pos + data_len);

        return true;
    }

    inline std::vector<std::byte> packMessage(const uint8_t *data, size_t len)
    {
        if (len > UINT32_MAX)
        {
            throw std::length_error("packMessage: len exceeds UINT32_MAX");
        }

        std::vector<std::byte> buf(len + 4);

        uint32_t net_len = htonl(static_cast<uint32_t>(len));
        memcpy(buf.data(), &net_len, 4);
        memcpy(buf.data() + 4, data, len);

        return buf;
    }

    // ---------- 解包结果 ----------
    struct UnpackedMessage
    {
        const uint8_t *payload;
        uint32_t len;
    };

    // ---------- 解包：从原始字节中取出 len + payload ----------
    // 返回 std::nullopt 表示数据不合法
    inline std::optional<UnpackedMessage> unpackMessage(const std::byte *data, size_t size)
    {
        if (size < 4)
        {
            PLOG_WARNING << "unpackMessage: data size is less than 4";
            return std::nullopt;
        }

        uint32_t net_len;
        memcpy(&net_len, data, 4);
        uint32_t len = ntohl(net_len);

        if (len > size - 4)
        {
            PLOG_WARNING << "unpackMessage: data size is less than len";
            return std::nullopt;
        }

        const uint8_t *payload = reinterpret_cast<const uint8_t *>(data) + 4;
        return UnpackedMessage{payload, len};
    }

} // namespace p2psocks
