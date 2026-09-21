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
    int stun_port = 3478;

    int ws_bind_port = 8080;
    int ws_max_message_size = 1024 * 1024;
    int ws_connection_timeout = 30;

    bool ws_enable_tls = false;

    std::string ws_certificate_pem_file;
    std::string ws_key_pem_file;
    std::string ws_key_pem_pass;
    std::string ws_bind_address = "0.0.0.0";

    std::string server_password;

    int asio_thread_count = 4;
};

Config H_config;

void readConfig()
{
    CSimpleIniA ini;

    // 允许使用 UTF-8
    ini.SetUnicode();

    if (!std::filesystem::exists("config.ini"))
    {
        PLOG_WARNING << "config.ini not found, creating default config";

        ini.SetValue("stun", "ip", "127.0.0.1");
        ini.SetLongValue("stun", "port", 3478);

        ini.SetLongValue("websocket", "bind_port", 8080);
        ini.SetLongValue("websocket", "max_message_size", 1024 * 1024);
        ini.SetLongValue("websocket", "connection_timeout", 30);
        ini.SetBoolValue("websocket", "enable_tls", false);
        ini.SetValue("websocket", "certificate_pem_file", "");
        ini.SetValue("websocket", "key_pem_file", "");
        ini.SetValue("websocket", "key_pem_pass", "");
        ini.SetValue("websocket", "bind_address", "0.0.0.0");

        ini.SetValue("server", "password", "");

        SI_Error status = ini.SaveFile("config.ini");

        if (status != SI_OK)
        {
            PLOG_ERROR << "create default config.ini failed, status=" << status;
            return;
        }

        PLOG_INFO << "default config.ini created";
    }
    else
    {
        SI_Error status = ini.LoadFile("config.ini");

        if (status != SI_OK)
        {
            PLOG_ERROR << "read config.ini failed, status=" << status;
            return;
        }
    }

    // STUN
    H_config.stun_ip =
        ini.GetValue("stun", "ip", "stun.miwifi.com:3478");

    H_config.stun_port =
        ini.GetLongValue("stun", "port", 3478);

    // WebSocket
    H_config.ws_bind_port =
        ini.GetLongValue("websocket", "bind_port", 8080);

    H_config.ws_max_message_size =
        ini.GetLongValue("websocket", "max_message_size", 1024 * 1024);

    H_config.ws_connection_timeout =
        ini.GetLongValue("websocket", "connection_timeout", 30);

    H_config.ws_enable_tls =
        ini.GetBoolValue("websocket", "enable_tls", false);

    H_config.ws_certificate_pem_file =
        ini.GetValue("websocket", "certificate_pem_file", "");

    H_config.ws_key_pem_file =
        ini.GetValue("websocket", "key_pem_file", "");

    H_config.ws_key_pem_pass =
        ini.GetValue("websocket", "key_pem_pass", "");

    H_config.ws_bind_address =
        ini.GetValue("websocket", "bind_address", "0.0.0.0");

    // Server
    H_config.server_password =
        ini.GetValue("server", "password", "");

    H_config.asio_thread_count =
        ini.GetLongValue("server", "asio_thread_count", 4);

    PLOG_INFO << "config loaded successfully";
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

        int thread_count = H_config.asio_thread_count;

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