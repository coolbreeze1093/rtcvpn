#pragma once
#include <functional>
#include "session_protocol.h"

class SessionInterface
{
    using CloseFunc = std::function<void()>;
    using SendDataFunc = std::function<void(const uint8_t *d, size_t n)>;
    using SendFinish = std::function<void()>;
    using SendSync = std::function<void(bool success)>;
    using SendDataCtrlFunc = std::function<void(p2psocks::CtrlType ctrl)>;

public:
    virtual ~SessionInterface() = default;
    virtual void stop(){
        is_p2p_closed_ = true;
        close();
    };
    virtual void close() = 0;
    virtual void start(const std::string &host, uint16_t port) = 0;
    virtual void revP2pData(const uint8_t *d, size_t n) = 0;
    virtual void start_receive() = 0;
    virtual void pause_receive() = 0;

    virtual void bind_close_func(CloseFunc close_func) { close_func_ = close_func; };
    virtual void bind_send_data(SendDataFunc send_data) { send_data_ = send_data; };
    virtual void bind_send_finish(SendFinish send_finish) { send_finish_ = send_finish; };
    virtual void bind_send_sync(SendSync send_sync) { send_synack_ = send_sync; };
    virtual void bind_send_data_ctrl(SendDataCtrlFunc send_data_ctrl) { send_data_ctrl_ = send_data_ctrl; };

    void send_data_ctrl(p2psocks::WriteQueueStatus ctrl)
    {
        if (ctrl == p2psocks::WriteQueueStatus::Danger)
        {
            send_data_ctrl_(p2psocks::CtrlType::pause);
        }
        else
        {
            send_data_ctrl_(p2psocks::CtrlType::receive);
        };
    }

    void closeSession()
    {
        if(is_closed_)
        {
            return;
        }
        
        if(!is_p2p_closed_)
        {
            send_finish_();
        }
        
        is_closed_ = true;

        if(close_func_)
        {
            close_func_();
        }
    };

protected:
    CloseFunc close_func_;
    SendDataFunc send_data_;
    SendFinish send_finish_;
    SendSync send_synack_;
    SendDataCtrlFunc send_data_ctrl_;

    bool is_p2p_closed_ = false;

    bool is_closed_ = false;
};