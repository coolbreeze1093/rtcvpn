#include "process_new_client.h"
#include <fstream>
#include "rtc_logger.h"
#include "SimpleIni.h"

std::atomic<bool> running{true};
void signal_handler(int signal)
{
    running = false;
}

struct Config
{
    std::string stun_ip;
    int stun_port;
    int ws_bind_port;
    int ws_max_message_size;
    int ws_connection_timeout;
    bool ws_enable_tls;
    std::string ws_certificate_pem_file;
    std::string ws_key_pem_file;
    std::string ws_key_pem_pass;
    std::string ws_bind_address;
    std::string server_password;
};

Config H_config;

void readConfig()
{
    CSimpleIniA ini;
    SI_Error status = ini.LoadFile("config.ini");
    if (status != SI_OK)
    {
        PLOG_ERROR << "read config file failed status=" << status;
        return;
    }



}

int main(int argc, char *argv[])
{
    RtcLogger::instance().init("rtc_server.log");
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    rtc::InitLogger(rtc::LogLevel::Debug, rtcLogCallback);
    PLOG_INFO << "RTC WebRTC C++ Server Started";

    rtc::Configuration config;
    config.iceServers = {
        {H_config.stun_ip, H_config.stun_port},
    };

    try
    {
        asio::io_context io;
        auto work_guard = asio::make_work_guard(io);
        ProcessNewWsClient processNewWsClient(io, config);
        rtc::WebSocketServerConfiguration ws_config;
        ws_config.port = H_config.ws_bind_port;
        ws_config.connectionTimeout = std::chrono::milliseconds(H_config.ws_connection_timeout);
        ws_config.maxMessageSize = H_config.ws_max_message_size;
        ws_config.bindAddress = H_config.ws_bind_address;
        ws_config.enableTls = H_config.ws_enable_tls;

        rtc::WebSocketServer rtc_ws_server(ws_config);
        rtc_ws_server.onClient([&processNewWsClient](std::shared_ptr<rtc::WebSocket> ws)
                               {
                                 processNewWsClient.newClient(ws);
                                 PLOG_DEBUG << "New WebSocket client"; });

        PLOG_INFO << "RTC WebRTC C++ Server Started...\n";

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