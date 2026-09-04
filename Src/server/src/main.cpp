#include "process_new_client.h"
#include <fstream>
#include "rtc_logger.h"

std::atomic<bool> running{true};
void signal_handler(int signal)
{
    running = false;
}

int main()
{
    RtcLogger::instance().init("rtc_server.log");
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    rtc::InitLogger(rtc::LogLevel::Debug, rtcLogCallback);
    PLOG_INFO << "RTC WebRTC C++ Server Started";

    rtc::Configuration config;
    config.iceServers = {
        {"stun.miwifi.com", 3478},
    };

    try
    {
        asio::io_context io;
        auto work_guard = asio::make_work_guard(io);
        ProcessNewWsClient processNewWsClient(io, config);

        rtc::WebSocketServer rtc_ws_server;
        rtc_ws_server.onClient([&processNewWsClient](std::shared_ptr<rtc::WebSocket> ws)
                               {
                                 processNewWsClient.newClient(ws);
                                 PLOG_DEBUG << "New WebSocket client"; });

        PLOG_INFO << "[远端] 已启动，等待本地端连接...\n";

        std::vector<std::thread> io_threads;

        int thread_count = 4; // 根据CPU核数或负载调整

        for (int i = 0; i < thread_count; i++)
        {
            io_threads.emplace_back([&io]()
                                    { io.run(); });
        }

        for (auto &t : io_threads)
        {
            if (t.joinable())
                t.join();
        }

        io.stop();
    }
    catch (std::exception &e)
    {
        PLOG_ERROR << "异常: " << e.what() << "\n";
    }
    RtcLogger::instance().shutdown();

    return 0;
}