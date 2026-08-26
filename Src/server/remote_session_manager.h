
#pragma once

#include <memory>
#include <unordered_map>

class RemoteSession;
class RemoteSessionManager
{
public:
    void register_session(uint32_t id,std::shared_ptr<RemoteSession> s)
    {
        sessions_map_[id] = s;
    }
    void remove_session(uint32_t id)
    {
        if(sessions_map_.count(id))
        sessions_map_.erase(id);
    }

private:
    std::unordered_map<uint32_t, std::shared_ptr<RemoteSession>> sessions_map_;
};