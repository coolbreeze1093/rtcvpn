#include "p2p_server.h"
#include "session_mux.h"
#include "process_send_data.h"
#include "remote.h"
#include "remote_session_manager.h"
#include "p2p_server_manager.h"

std::atomic<bool> running{true};
void signal_handler(int signal)
{
    running = false;
}

class ProcessNewWsClient
{
public:
    ProcessNewWsClient(asio::io_context &io, rtc::Configuration config) : mux_(peer_conn_id), io_(io), config_(config)
    {
        mux_.set_on_syn([&](std::shared_ptr<Session> session,
                            const std::string &host, uint16_t port)
                        {
            std::cout << "[远端] 收到连接请求 " << host << ":" << port
                      << " (stream_id=" << session->stream_id() << ")\n";
            auto rs = std::make_shared<RemoteSession>(io_, mux_, session, sessions_keepalive_);
            rs->connect_target(host, port); });
    }

    void newClient(std::shared_ptr<rtc::WebSocket> ws)
    {
        UUIDv4::UUIDGenerator<std::mt19937_64> uuidGenerator;
        UUIDv4::UUID uuid = uuidGenerator.getUUID();
        std::string uuid_str = uuid.bytes();

        auto ws_s = std::make_shared<ws_server>(uuid_str);
        ws_servers_[uuid_str] = ws_s;
        ws_s->bindLoginSuccess([this](std::string uuid)
                               {
                                std::shared_ptr<ws_server> ws_s = ws_servers_[uuid];
                          auto p2p_s = std::make_shared<p2p_server>(uuid);  
                            p2p_s->init(this->config_);
                            p2p_s->bindWsSend(std::bind(&ws_server::send,ws_s,std::placeholders::_1));
                            ws_s->bindsetRemoteDescriptionFunc(std::bind(&p2p_server::setRemoteDescription,p2p_s,std::placeholders::_1,std::placeholders::_2));
                            ws_s->bindaddRemoteCandidateFunc(std::bind(&p2p_server::addRemoteCandidate,p2p_s,std::placeholders::_1,std::placeholders::_2));
                            ws_s->bindCloseFunc([this](std::string uuid){
                                ws_servers_.erase(uuid);
                                std::cout<<"ws_server::~ws_server()  "<<uuid<<std::endl;
                            });
                            p2p_servers_[uuid] = p2p_s;
                            process_send_datas_[uuid] = std::make_shared<ProcessSendData>(p2p_s, this->mux_);
                            p2p_s->bindCloseFunc([this](std::string uuid){
                                process_send_datas_.erase(uuid);
                                p2p_servers_.erase(uuid);
                                std::cout<<"p2p_server::~p2p_server()  "<<uuid<<std::endl;
                            }); });
        ws_s->connect(ws);
        std::cout << "newClient  " << uuid_str << std::endl;
    }

private:
    RemoteSessionManager sessions_keepalive_;
    uint32_t peer_conn_id = 1; // 占位，换成你的真实值
    SessionMux mux_;
    p2pServerManager registerServer_;
    asio::io_context &io_;
    rtc::Configuration config_;

    std::unordered_map<std::string, std::shared_ptr<p2p_server>> p2p_servers_;
    std::unordered_map<std::string, std::shared_ptr<ws_server>> ws_servers_;
    std::unordered_map<std::string, std::shared_ptr<ProcessSendData>> process_send_datas_;
};

int main()
{
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "RTC WebRTC C++" << std::endl;

    rtc::InitLogger(rtc::LogLevel::Info);
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
                        
        std::cout << "New WebSocket client" << std::endl; });

        std::cout << "[远端] 已启动，等待本地端连接...\n";

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

    return 0;
}
