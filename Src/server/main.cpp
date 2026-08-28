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

#include <asio.hpp>
#include <iostream>
#include <array>

using asio::ip::udp;

class UdpClient: public std::enable_shared_from_this<UdpClient>
{
public:
    UdpClient(asio::io_context& io_context,SessionMux& mux,
               const std::string& local_host, uint16_t local_port,
               const std::string& remote_host, uint16_t remote_port,
               uint32_t stream_id)
               : socket_(io_context, udp::endpoint(udp::v4(), 0))
               ,mux_(mux)
               ,timer_(io_context)
               ,local_host_(local_host)
               ,local_port_(local_port)
               ,remote_host_(remote_host)
               ,remote_port_(remote_port)
               ,stream_id_(stream_id)
               ,recv_buf_(65536)
    {
        udp::resolver resolver(io_context);
        server_endpoint_ = *resolver.resolve(udp::v4(), remote_host_, std::to_string(remote_port_)).begin();
    }
    ~UdpClient()
    {
        socket_.close();
    }

    void send(const std::vector<uint8_t>& data)
    {
        auto self = shared_from_this();
        socket_.async_send_to(
            asio::buffer(data), server_endpoint_,
            [self](std::error_code ec, std::size_t bytes_sent)
            {
                if (ec)
                {
                    std::cout << "send error: " << ec.message() << std::endl;
                    return;
                }
                std::cout << "Sent " << bytes_sent << " bytes" << std::endl;
                self->start_receive();
            });
    }

private:
    void start_receive()
    {
        auto self = shared_from_this();
        timer_.expires_after(std::chrono::seconds(10));
        timer_.async_wait([self](std::error_code ec)
        {
            if (!ec)  // 定时器正常触发(没有被取消),说明超时了
            {
                std::cout << "UDP receive timeout, closing. stream_id=" << self->stream_id_ << std::endl;
                self->socket_.close();  // 这会让 async_receive_from 以 operation_aborted 返回
            }
        });
        socket_.async_receive_from(
            asio::buffer(recv_buf_), sender_endpoint_,
            [self](std::error_code ec, std::size_t bytes_recvd)
            {
                self->timer_.cancel();

                if (!ec)
                {
                    std::vector<uint8_t> reply(self->recv_buf_.begin(), self->recv_buf_.begin() + bytes_recvd);
                    self->mux_.send_udp_reply(self->stream_id_,
                                               self->local_host_, self->local_port_,
                                               self->remote_host_, self->remote_port_,
                                               reply);
                    self->start_receive();
                }
                else
                {
                    std::cout << "receive error: " << ec.message() << std::endl;
                }
            });
    }

    udp::socket socket_;
    udp::endpoint server_endpoint_;
    udp::endpoint sender_endpoint_;
    asio::steady_timer timer_;
    std::vector<uint8_t> recv_buf_;

    
    SessionMux& mux_;
    std::string local_host_;
    uint16_t local_port_;
    std::string remote_host_;
    uint16_t remote_port_;
    uint32_t stream_id_;
    
};

class ProcessNewWsClient
{
public:
    ProcessNewWsClient(asio::io_context &io, rtc::Configuration config) : mux_(peer_conn_id), io_(io), config_(config)
    {
        mux_.set_on_syn([this](std::shared_ptr<Session> session,
                            const std::string &host, uint16_t port)
                        {
            std::cout << "[远端] 收到连接请求 " << host << ":" << port
                      << " (stream_id=" << session->stream_id() << ")\n";
            auto rs = std::make_shared<RemoteSession>(io_, mux_, session, sessions_keepalive_);
            rs->connect_target(host, port); });

        mux_.set_on_udp([this](uint32_t stream_id,const std::string&local_host, uint16_t local_port,
                            const std::string&remote_host, uint16_t remote_port,
                            const std::vector<uint8_t>& data)
                        {
                            std::cout << "[远端] 收到UDP数据 " << local_host << ":" << local_port
                                      << " -> " << remote_host << ":" << remote_port
                                      << " (stream_id=" << stream_id << ")\n";
                            auto udp = std::make_shared<UdpClient>(io_, mux_, local_host, local_port, remote_host, remote_port, stream_id);
                            udp->send(data);
                        });
    }
    //新连接的客户端
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
