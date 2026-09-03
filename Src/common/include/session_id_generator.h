#pragma once
#include <cstdint>
#include <plog/Log.h>

class SessionIdGenerator
{
public:
    SessionIdGenerator() = default;
    ~SessionIdGenerator() = default;
    uint32_t create_session_id() { 
        if(id_>65535)
        {
            PLOG_INFO << "SessionIdGenerator: id_ overflow, reset to 0";
            id_ = 0;
        }
        return ++id_;
     }
private:
    uint32_t id_ = 0;
};