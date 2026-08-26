#pragma once

#include <memory>
#include <unordered_map>
class p2p_server;
class ws_server;
class p2pServerManager {
public:
    p2pServerManager()
    {
        
    }

    void registerWsServer(const std::string& uuid,std::shared_ptr<ws_server> s)
    {
        ws_servers_[uuid] = s;
    }

    void removeWsServer(const std::string& uuid)
    {
        ws_servers_.erase(uuid);
    }

    void registerP2pServer(const std::string& uuid,std::shared_ptr<p2p_server> s)
    {
        p2p_servers_[uuid] = s;
    }
    void removeP2pServer(const std::string& uuid)
    {
        if(p2p_servers_.count(uuid))
        p2p_servers_.erase(uuid);
    }

    private:
    std::unordered_map<std::string, std::shared_ptr<p2p_server>> p2p_servers_;
    std::unordered_map<std::string, std::shared_ptr<ws_server>> ws_servers_;
};