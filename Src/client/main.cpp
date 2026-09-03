#include <csignal>
#include <asio.hpp>
#include "socks5.h"
#include "p2p_client.h"
#include <plog/Log.h>

std::atomic<bool> running{true};
void signal_handler(int signal)
{
    running = false;
}
int main(int argc, char *argv[])
{
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    PLOG_INFO << "RTC WebRTC C++";
    rtc::InitLogger(rtc::LogLevel::Info);
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

    return 0;
}