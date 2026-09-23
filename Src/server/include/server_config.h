#pragma once
#include <string>
#include <filesystem>
#include "Simpleini.h"
#include "plog/log.h"

struct ServerConfig
{
    std::string stun_ip;
    uint16_t stun_port = 3478;

    uint16_t ws_bind_port = 8080;
    uint32_t ws_max_message_size = 1024 * 1024;
    uint16_t ws_connection_timeout = 10000;

    bool ws_enable_tls = false;

    std::string ws_certificate_pem_file;
    std::string ws_key_pem_file;
    std::string ws_key_pem_pass;
    std::string ws_bind_address = "0.0.0.0";

    std::string server_password;

    int asio_thread_count = 4;

    static void readConfig(ServerConfig &config, std::string exec_dir)
    {
        CSimpleIniA ini;

        // 允许使用 UTF-8
        ini.SetUnicode();

        if (!std::filesystem::exists(exec_dir))
        {
            PLOG_WARNING << "config.ini not found, creating default config";

            ini.SetValue("stun", "ip", config.stun_ip.c_str());
            ini.SetLongValue("stun", "port", config.stun_port);

            ini.SetLongValue("websocket", "bind_port", config.ws_bind_port);
            ini.SetLongValue("websocket", "max_message_size", config.ws_max_message_size);
            ini.SetLongValue("websocket", "connection_timeout", config.ws_connection_timeout);
            ini.SetBoolValue("websocket", "enable_tls", config.ws_enable_tls);
            ini.SetValue("websocket", "certificate_pem_file", config.ws_certificate_pem_file.c_str());
            ini.SetValue("websocket", "key_pem_file", config.ws_key_pem_file.c_str());
            ini.SetValue("websocket", "key_pem_pass", config.ws_key_pem_pass.c_str());
            ini.SetValue("websocket", "bind_address", config.ws_bind_address.c_str());

            ini.SetValue("server", "password", config.server_password.c_str());
            ini.SetLongValue("server", "asio_thread_count", config.asio_thread_count);

            SI_Error status = ini.SaveFile(exec_dir.c_str());

            if (status != SI_OK)
            {
                PLOG_ERROR << "create default config.ini failed, status=" << status;
                return;
            }

            PLOG_INFO << "default config.ini created";
        }
        else
        {
            SI_Error status = ini.LoadFile(exec_dir.c_str());

            if (status != SI_OK)
            {
                PLOG_ERROR << "read config.ini failed, status=" << status;
                return;
            }
        }

        // STUN
        config.stun_ip =
            ini.GetValue("stun", "ip", "stun.miwifi.com:3478");

        config.stun_port =
            ini.GetLongValue("stun", "port", 3478);

        // WebSocket
        config.ws_bind_port =
            ini.GetLongValue("websocket", "bind_port", 8080);

        config.ws_max_message_size =
            ini.GetLongValue("websocket", "max_message_size", 1024 * 1024);

        config.ws_connection_timeout =
            ini.GetLongValue("websocket", "connection_timeout", 30);

        config.ws_enable_tls =
            ini.GetBoolValue("websocket", "enable_tls", false);

        config.ws_certificate_pem_file =
            ini.GetValue("websocket", "certificate_pem_file", "");

        config.ws_key_pem_file =
            ini.GetValue("websocket", "key_pem_file", "");

        config.ws_key_pem_pass =
            ini.GetValue("websocket", "key_pem_pass", "");

        config.ws_bind_address =
            ini.GetValue("websocket", "bind_address", "0.0.0.0");

        config.server_password =
            ini.GetValue("server", "password", "");

        config.asio_thread_count =
            ini.GetLongValue("server", "asio_thread_count", 4);

        PLOG_INFO << "config loaded successfully";
    }
};
