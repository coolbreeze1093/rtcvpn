#include "p2p_server.h"
#include "session_mux.h"
#include "process_send_data.h"
#include "remote.h"

std::atomic<bool> running{true};
void signal_handler(int signal)
{
    running = false;
}

class SessionManager
{
public:
    SessionManager() = default;
    ~SessionManager() = default;
    uint32_t create_session_id() { 
        if(id_>65535)
            id_ = 0;
        return ++id_;
     }
    void register_tcp_session(std::shared_ptr<RemoteSession> s)
    {
        uint32_t id = create_session_id();
        sessions_[id] = s;
        s->set_session_id(id);
    }
    void remove_tcp_session(uint32_t stream_id) { sessions_.erase(stream_id); }

    void register_udp_session(std::shared_ptr<UdpClient> s)
    {
        uint32_t id = create_session_id();
        udp_sessions_[id] = s;
        s->set_session_id(id);
    }
    void remove_udp_session(uint32_t stream_id) { udp_sessions_.erase(stream_id); }

private:
    std::unordered_map<uint32_t, std::shared_ptr<RemoteSession>> sessions_;
    std::unordered_map<uint32_t, std::shared_ptr<UdpClient>> udp_sessions_;
    uint32_t id_ = 0;
};

class ProcessNewWsClient
{
public:
    ProcessNewWsClient(asio::io_context &io, rtc::Configuration config) : mux_(peer_conn_id), io_(io), config_(config)
    {
        mux_.set_on_syn([this](uint32_t session_id,
                               const std::string &host, uint16_t port)
                        {
            std::cout << "[远端] 收到连接请求 " << host << ":" << port
                      << " (stream_id=" << session_id << ")\n";
            auto rs = std::make_shared<RemoteSession>(io_, mux_, session_id);
            sessions_Manager_.register_tcp_session(rs);
            rs->bind_close_func([this](uint32_t session_id)
                                  {
                                      sessions_Manager_.remove_tcp_session(session_id);
                                  });
            
            rs->connect_target(host, port); });

        mux_.set_on_udp_syn([this](uint32_t session_id)
                            {
                            std::cout << "[远端] 收到UDP数据 " 
                                      << " (stream_id=" << session_id << ")\n";
                            auto udp = std::make_shared<UdpClient>(io_, mux_, session_id);
                            sessions_Manager_.register_udp_session(udp);
                            udp->bind_close_func([this](uint32_t session_id)
                                  {
                                      sessions_Manager_.remove_udp_session(session_id);
                                  });
                            udp->start(); });
    }
    // 新连接的客户端
    void newClient(std::shared_ptr<rtc::WebSocket> ws)
    {
        uint32_t id = create_session_id();
        auto ws_s = std::make_shared<ws_server>(id);
        ws_servers_[id] = ws_s;
        ws_s->bindLoginSuccess([this](uint32_t session_id)
                               {
                                std::shared_ptr<ws_server> ws_s = ws_servers_[session_id];
                          auto p2p_s = std::make_shared<p2p_server>(session_id);  
                            p2p_s->init(this->config_);
                            p2p_s->bindWsSend(std::bind(&ws_server::send,ws_s,std::placeholders::_1));
                            ws_s->bindsetRemoteDescriptionFunc(std::bind(&p2p_server::setRemoteDescription,p2p_s,std::placeholders::_1,std::placeholders::_2));
                            ws_s->bindaddRemoteCandidateFunc(std::bind(&p2p_server::addRemoteCandidate,p2p_s,std::placeholders::_1,std::placeholders::_2));
                            ws_s->bindCloseFunc([this](uint32_t session_id){
                                ws_servers_.erase(session_id);
                                std::cout<<"ws_server::~ws_server()  "<<session_id<<std::endl;
                            });
                            p2p_servers_[session_id] = p2p_s;
                            process_send_datas_[session_id] = std::make_shared<ProcessSendData>(p2p_s, this->mux_);
                            p2p_s->bindCloseFunc([this](uint32_t session_id){
                                process_send_datas_.erase(session_id);
                                p2p_servers_.erase(session_id);
                                std::cout<<"p2p_server::~p2p_server()  "<<session_id<<std::endl;
                            }); });
        ws_s->connect(ws);
        std::cout << "newClient  " << id << std::endl;
    }

    uint32_t create_session_id() { 
        if(id_>65535)
            id_ = 0;
        return ++id_;
     }

private:
    SessionManager sessions_Manager_;
    uint32_t peer_conn_id = 1; // 占位，换成你的真实值
    SessionMux mux_;
    asio::io_context &io_;
    rtc::Configuration config_;

    std::unordered_map<uint32_t, std::shared_ptr<p2p_server>> p2p_servers_;
    std::unordered_map<uint32_t, std::shared_ptr<ws_server>> ws_servers_;
    std::unordered_map<uint32_t, std::shared_ptr<ProcessSendData>> process_send_datas_;

    uint32_t id_ = 0;
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
