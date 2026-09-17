#pragma once

#include <asio.hpp>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "session_protocol.h"

namespace p2psocks
{

// UdpSocket 是一个完整的 UDP 收发消息通道：
//   - start() 打开并绑定本地 UDP socket，开始接收数据，
//     结果通过 OpenCallback 通知
//   - 收到任意对端发来的数据时，通过 DataCallback 回调给外部
//     （因为 UDP 无连接，回调里会带上发送方的 host/port）
//   - send() 把数据发往指定的 target_host:target_port，
//     内部排队、串行发送（每条待发数据都会先做一次域名解析）
//   - close() 发起关闭；内部保证把发送队列中尚未发出的数据
//     全部发送完毕后，才真正关闭 socket 并触发 CloseCallback
//
// 线程安全：start() / send() / close() 可以从任意线程调用，
// 包括 io_context 自身的多个工作线程。内部所有状态都通过
// asio::strand 串行化——公开接口用 asio::post(strand_, ...)
// 把实际操作丢进 strand，socket_ / resolver 也绑定在同一个
// strand 上，因此它们的异步操作完成回调也会在这个 strand 里
// 触发，不需要锁。
class UdpSocket : public std::enable_shared_from_this<UdpSocket>
{
public:
    using OpenCallback = std::function<void(bool success)>;
    using DataCallback = std::function<void(const std::string &remote_host,
                                             uint16_t remote_port,
                                             const uint8_t *data, size_t len)>;
    using CloseCallback = std::function<void()>;
    using WriteQueueCallback = std::function<void(WriteQueueStatus queue)>;

    explicit UdpSocket(asio::io_context &io);
    ~UdpSocket();

    // 设置打开结果回调（在 start() 之前设置）
    void setOpenCallback(OpenCallback cb);

    // 设置收到数据时的回调
    void setDataCallback(DataCallback cb);

    // 设置关闭完成后的回调（保证在队列数据发送完之后才触发）
    void setCloseCallback(CloseCallback cb);

    // 设置发送队列状态回调
    void setWriteQueueCallback(WriteQueueCallback cb);

    // 打开本地 UDP socket 并绑定端口（local_port 为 0 表示由系统
    // 随机分配），成功后自动开始接收数据。
    void start(uint16_t local_port = 0);

    // 发送数据接口：把 data 发往 target_host:target_port。
    // 若尚未打开成功，或已经处于关闭流程中，数据会被丢弃。
    void send(std::shared_ptr<std::vector<uint8_t>> data,
              const std::string &target_host, uint16_t target_port);

    // 便捷重载：内部会拷贝一份 data
    void send(const uint8_t *data, size_t len,
              const std::string &target_host, uint16_t target_port);

    // 关闭接口：
    //   - 如果发送队列非空，会等待队列中的数据全部发送完毕
    //     （或者发送过程中出错）后再真正关闭 socket
    //   - 如果发送队列为空，立即关闭
    //   - CloseCallback 保证只会被调用一次
    //   - 如果在打开尚未成功时调用，直接关闭，不会触发 CloseCallback
    //     （打开失败的结果由 OpenCallback 表达）
    void close();

    void startReading();

    void stopReading();

    int getLocalPort();

    std::string get_local_ip();

private:
    struct PendingSend
    {
        std::string target_host;
        uint16_t target_port;
        std::shared_ptr<std::vector<uint8_t>> data;
    };

    void do_send_next();
    void do_read();
    void do_close();

    asio::io_context &io_;

    // 所有跟这个 session 相关的异步操作和状态变更都发生在这个
    // strand 上，用来在多线程 io_context 下代替锁。
    asio::strand<asio::io_context::executor_type> strand_;

    asio::ip::udp::socket socket_;
    asio::ip::udp::endpoint sender_endpoint_;
    std::vector<uint8_t> recv_buf_;

    bool is_open_ = false;

    std::deque<PendingSend> send_queue_;
    bool is_sending_ = false;

    bool is_closing_ = false; // 已经调用过 close()，正在等待队列发完
    bool is_closed_ = false;  // 已经真正关闭并回调过 CloseCallback

    OpenCallback open_callback_;
    DataCallback data_callback_;
    CloseCallback close_callback_;
    WriteQueueCallback write_queue_callback_;

    bool is_reading_ = true;
};

} // namespace p2psocks
