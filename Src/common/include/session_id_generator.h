#pragma once
#include <cstdint>
#include <plog/Log.h>

class SessionIdGenerator
{
public:
    SessionIdGenerator() = default;
    ~SessionIdGenerator() = default;
    uint32_t create_session_id();
private:
    uint32_t id_ = 0;
};