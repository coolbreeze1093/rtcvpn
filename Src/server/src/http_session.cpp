#include "http_session.h"
#include <plog/Log.h>
#include <asio.hpp>

using asio::ip::tcp;
using namespace p2psocks;

HttpSession::HttpSession(asio::io_context &io)
    : target_socket_(std::make_shared<TcpSocket>(io)),
      http_parse_request_(HttpParser::Type::Request)
{
    PLOG_DEBUG << "HttpSession created";

    http_parse_request_.set_on_headers_complete([this](HttpParser::Message &msg)
                                                {
                                                    msg.target = ToOriginForm(msg.target);
                                                      std::string s = msg.serialize_headers_only();
                                                      //PLOG_DEBUG << "http headers: " << s;
                                                      std::vector<uint8_t> v(s.size());
                                                      std::memcpy(v.data(), s.data(), s.size());
                                                      target_socket_->send(v.data(), v.size()); });
    http_parse_request_.set_on_body([this](const char *data, std::size_t len)
                                    {
                                  //PLOG_DEBUG << "http body: " << std::string(data, len);
                                  if (http_parse_request_.message().chunked)
    {
        // 重新编码为 chunked 格式: <hex-length>\r\n<data>\r\n
        std::ostringstream oss;
        oss << std::hex << len << "\r\n";
        std::string chunk_header = oss.str();

        std::vector<uint8_t> v;
        v.reserve(chunk_header.size() + len + 2);

        // chunk size 行
        v.insert(v.end(), chunk_header.begin(), chunk_header.end());
        // chunk data
        v.insert(v.end(), data, data + len);
        // 结尾 CRLF
        v.push_back('\r');
        v.push_back('\n');

        target_socket_->send(v.data(), v.size());
    }
    else
    {
        target_socket_->send(reinterpret_cast<const uint8_t *>(data), len);
    } });

    http_parse_request_.set_on_error([this](int errno_code, std::string_view reason)
                                     {
                                    PLOG_ERROR << "http parse error: " << errno_code << " reason: " << reason.data();
                                    close(); });

    http_parse_request_.set_on_message_complete([this]()
                                                {
                                                    if(http_parse_request_.message().chunked)
                                                    {
                                                        static const std::vector<uint8_t> v{'0', '\r', '\n', '\r', '\n'};
                                                        target_socket_->send(v.data(), v.size()); 
                                                    } });

    

}

HttpSession::~HttpSession()
{
    PLOG_DEBUG << "HttpSession close";
}

void HttpSession::start(const std::string &host, uint16_t port)
{
    auto self = shared_from_this();
    target_socket_->setCloseCallback([this,self](){
        closeSession(); 
        target_socket_.reset();
    });
    target_socket_->setConnectCallback([this,self](bool success){ send_synack_(success); });
    target_socket_->setDataCallback([this,self](const uint8_t *d, size_t n){ send_data_(d, n); });
    target_socket_->setWriteQueueCallback([this,self](WriteQueueStatus queue){ send_data_ctrl(queue); });
    target_socket_->connect(host, port);
}

void HttpSession::close()
{
    target_socket_->close();
    PLOG_INFO << "HttpSession close";
}

void HttpSession::revP2pData(const uint8_t *d, size_t n)
{
    http_parse_request_.feed(reinterpret_cast<const char *>(d), n);
}

void HttpSession::start_receive()
{
    target_socket_->startReading();
}

void HttpSession::pause_receive()
{
    target_socket_->stopReading();
}
