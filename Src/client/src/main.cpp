#include <csignal>
#include <asio.hpp>
#include "socks5.h"
#include "p2p_client.h"
#include <plog/Log.h>
#include <fstream>
#include "rtc_logger.h"

std::atomic<bool> running{true};
void signal_handler(int signal)
{
    running = false;
}

class NetworkRtcApp
{
    public:
        NetworkRtcApp(const std::string &signalingUrl,
              const std::vector<std::pair<std::string, uint16_t>> &stunServers,
              uint32_t peerConnId,asio::io_context & io_context);
        ~NetworkRtcApp() = default;
        void stop();
        void start();

    private:
        std::string signaling_url_;
        std::vector<std::pair<std::string, uint16_t>> stun_servers_;
        uint32_t peer_conn_id_ = 0;
        bool disconnected_ = false;
        p2p_client client_;
        SessionMux mux_;
        SocksServer server_;
        uint16_t socks5_server_port_ = 0;
        asio::io_context & io_context_;

};

NetworkRtcApp::NetworkRtcApp(const std::string &signalingUrl,
              const std::vector<std::pair<std::string, uint16_t>> &stunServers,
              uint32_t peerConnId,asio::io_context & io_context):
              signaling_url_(signalingUrl)
              ,stun_servers_(stunServers)
              ,io_context_(io_context)
              ,mux_(peer_conn_id_)
              ,server_(io_context_,socks5_server_port_,mux_)
{
    peer_conn_id_ = peerConnId;

    RtcLogger::instance().init("rtc_client.log");
    rtc::InitLogger(rtc::LogLevel::Debug, rtcLogCallback);

    rtc::Configuration config;
    for (const auto &[host, port] : stunServers)
    {
        config.iceServers.push_back({host, port});
    }

    client_.init(config);
    client_.connect(signalingUrl);
}

void NetworkRtcApp::stop()
{
    server_.stop();
}

void NetworkRtcApp::start()
{
    server_.start();
}



int main(int argc, char *argv[])
{
    RtcLogger::instance().init("rtc_client.log");
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    PLOG_INFO << "RTC WebRTC C++";
    rtc::InitLogger(rtc::LogLevel::Debug, rtcLogCallback);
    rtc::Configuration config;
    config.iceServers = {
        {"stun.miwifi.com", 3478},
    };
    p2p_client client;
    client.init(config);
    client.connect("ws://localhost:8080");

    uint16_t socks_port = 10800;
    
    try
    {
        asio::io_context io;
        auto work_guard = asio::make_work_guard(io);

        uint32_t peer_conn_id = 1; // 占位，换成你的真实值
        SessionMux mux(peer_conn_id);

        client.bindDataChannel([&mux](rtc::binary data)
                                 {
                        auto result = p2psocks::unpackMessage(data.data(), data.size());
                        if (!result)
                        {
                            return;
                        }

                    mux.on_p2p_data(1, result->payload, result->len);
                });
        mux.set_send_func([&client](uint32_t conn_id, const uint8_t *data, size_t len)
                           {
            std::vector<std::byte> buf;
            try
            {
                buf = p2psocks::packMessage(data, len);
            }
            catch (const std::length_error &e)
            {
                PLOG_ERROR << "sendData: " << e.what();
                return;
            }
            client.send(buf.data(), buf.size());
             });
        
        SocksServer server(io, socks_port, mux);
        
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
        std::cerr << "异常: " << e.what() << "\n";
    }

    client.disconnect();
    RtcLogger::instance().shutdown();

    return 0;
}