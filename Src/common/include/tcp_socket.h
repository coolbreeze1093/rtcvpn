#pragma once

#include <asio.hpp>
#include <array>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "session_protocol.h"

namespace p2psocks
{

// TcpSocket 是一个完整的 TCP 消息收发通道：
//   - connect() 负责域名解析 + 建立连接，通过 ConnectCallback 通知结果
//   - 连接成功后自动开始接收数据，通过 DataCallback 回调给外部
//   - send() 发送数据，内部排队、串行写出
//   - close() 发起关闭；内部保证把发送队列中尚未发出的数据
//     全部发送完毕后，才真正关闭 socket 并触发 CloseCallback
//
// 线程安全：connect() / send() / close() 可以从任意线程调用，
// 包括 io_context 自身的多个工作线程。内部所有状态
// （write_queue_、is_connected_、is_closing_ 等）都通过
// asio::strand 串行化——公开接口用 asio::post(strand_, ...)
// 把实际操作丢进 strand，socket_ / resolver 也绑定在同一个
// strand 上，因此它们的异步操作完成回调也会在这个 strand 里
// 触发。这样即使 io_context::run() 被多个线程同时调用，
// 同一个 TcpSocket 的所有内部逻辑也不会并发执行，不需要锁。
class TcpSocket : public std::enable_shared_from_this<TcpSocket>
{
public:
    using ConnectCallback = std::function<void(bool success)>;
    using DataCallback = std::function<void(const uint8_t *data, size_t len)>;
    using CloseCallback = std::function<void()>;
    using WriteQueueCallback = std::function<void(WriteQueueStatus queue)>;

    // 用一个已经建立好连接的外部 socket 构造 TcpSession。
    // 注意：传入的 socket 会被“转移”进来（其底层 native handle
    // 被搬运到本对象内部绑定在 strand_ 上的 socket），构造之后
    // 传入的 socket 对象本身不再持有连接，请勿再使用它。
    // 用这种方式构造后，调用 start() 而不是 connect() 开始收数据。
    TcpSocket(asio::io_context &io, asio::ip::tcp::socket socket);

    explicit TcpSocket(asio::io_context &io);
    ~TcpSocket();

    // 设置连接结果回调（在 connect() 之前设置；用外部 socket
    // 构造的场景不会触发这个回调，因为连接已经建立好了）
    void setConnectCallback(ConnectCallback cb);

    // 设置收到数据时的回调
    void setDataCallback(DataCallback cb);

    // 设置关闭完成后的回调（保证在队列数据发送完之后才触发）
    void setCloseCallback(CloseCallback cb);

    void setWriteQueueCallback(WriteQueueCallback cb);

    // 发起连接：解析 host:port 并建立 TCP 连接。
    // 连接成功后会自动开始接收对端数据。
    void connect(const std::string &host, uint16_t port);

    // 当使用“外部已连接 socket”的构造函数时，调用这个接口开始
    // 接收对端数据（等价于 connect() 成功后自动触发的 do_read()）。
    // 如果是通过 connect() 建立的连接，不需要调用这个方法。
    void start();

    // 发送数据接口。若尚未连接成功，或已经处于关闭流程中，
    // 新的数据会被丢弃。
    void send(const uint8_t *data, size_t len);

    // 关闭接口：
    //   - 如果发送队列非空，会等待队列中的数据全部发送完毕
    //     （或者发送过程中出错）后再真正关闭 socket
    //   - 如果发送队列为空，立即关闭
    //   - CloseCallback 保证只会被调用一次
    //   - 如果在连接尚未成功建立时调用，直接关闭，不会触发 CloseCallback
    //     （连接失败/取消的结果由 ConnectCallback 表达）
    void close();

    void startReading();

    void stopReading();

private:
    void do_read();
    void do_write();
    void do_close();

    asio::io_context &io_;

    // 所有跟这个 session 相关的异步操作和状态变更都发生在这个
    // strand 上，用来在多线程 io_context 下代替锁。
    asio::strand<asio::io_context::executor_type> strand_;

    asio::ip::tcp::socket socket_;

    std::string host_;
    uint16_t port_ = 0;

    bool is_connected_ = false;

    std::deque<std::vector<uint8_t>> write_queue_;
    bool is_sending_ = false;

    bool is_closing_ = false; // 已经调用过 close()，正在等待队列发完
    bool is_closed_ = false;  // 已经真正关闭并回调过 CloseCallback

    std::array<uint8_t, 8192> read_buf_;

    ConnectCallback connect_callback_;
    DataCallback data_callback_;
    CloseCallback close_callback_;
    WriteQueueCallback write_queue_callback_;

    WriteQueueStatus write_queue_status_ = WriteQueueStatus::Safety;

    bool is_reading_ = true;
};

} // namespace p2psocks