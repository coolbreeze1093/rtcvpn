#include <csignal>
#include <asio.hpp>
#include <plog/Log.h>
#include <fstream>
#include "rtc_logger.h"
#include "network_rtc_app.h"

#include "crash_dump.h"

std::atomic<bool> running{true};
void signal_handler(int signal)
{
    running = false;
}

void input_keyboard()
{
    char c;

    while (running && std::cin.get(c))
    {
        if (c == 'q' || c == 'Q')
        {
            PLOG_INFO << "input q, exit";

            running = false;

            // 如果 NetworkRtcApp 有 close/stop 方法
            // app.close();
            // 或者：
            // app.stop();

            break;
        }
    }
}

int main(int argc, char *argv[])
{
    CrashDump::InstallCrashHandler("");

    RtcLogger::instance().init("rtc_client.log");

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    PLOG_INFO << "RTC WebRTC C++";
    rtc::InitLogger(rtc::LogLevel::Debug, rtcLogCallback);

    std::string password_ = "test";
    std::string signaling_url_ = "ws://localhost:8080";
    uint16_t socks5_server_port = 10801;
    std::string stun_url_ = "stun.miwifi.com:3478";
    std::vector<std::pair<std::string, uint16_t>> stun_servers = {
        {stun_url_, 3478},
    };
    uint32_t peer_conn_id = 1;
    int thread_count = 4;

    asio::io_context io;
    // 防止 io.run() 因为暂时没有任务而直接退出
    auto work_guard = asio::make_work_guard(io);

    std::vector<std::thread> io_threads;

    for (int i = 0; i < thread_count; i++)
    {
        io_threads.emplace_back([&io]()
                                {
            io.run();
            /* try
            {
                io.run();
            }
            catch (const std::exception &e)
            {
                PLOG_ERROR << "io thread exception: " << e.what();
            } */ });
    }

    NetworkRtcApp::Config config;

    config.signalingUrl = signaling_url_;
    config.stunServers = stun_servers;
    config.password = password_;
    config.socks5_server_port = socks5_server_port;

    NetworkRtcApp app(peer_conn_id,io);

    app.onClose([&]()
                {
                    if(running==true)
                    {
                        PLOG_INFO << "p2p socks5 closed, please input q to exit.";
                        running = false;
                    }
                    else
                    {
                        PLOG_INFO << "p2p socks5 closed, exit.";
                    }
         });

    app.start(config);

    input_keyboard();

    PLOG_INFO << "closing ...";

    // 先关闭业务
    // 如果你的 NetworkRtcApp 有 stop() / close()，这里调用
    app.stop();

    // 允许 io.run() 退出
    work_guard.reset();

    for (auto &t : io_threads)
    {
        if (t.joinable())
            t.join();
    }

    RtcLogger::instance().shutdown();

    return 0;
}