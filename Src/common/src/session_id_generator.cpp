#include "session_id_generator.h"
#include <plog/Log.h>

uint32_t SessionIdGenerator::create_session_id()
{
    if(id_>65535)
    {
        PLOG_INFO << "SessionIdGenerator: id_ overflow, reset to 0";
        id_ = 0;
    }
    return ++id_;
}