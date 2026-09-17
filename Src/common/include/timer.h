#include <asio.hpp>
#include <iostream>
#include <chrono>
#include <memory>
#include <plog/Log.h>

class Timer : public std::enable_shared_from_this<Timer>
{
public:
    using TimerFinishCallback = std::function<void()>;

    Timer(asio::io_context& io)
        : timer_(io)
    {
        PLOG_DEBUG << "Timer created";
    }

    ~Timer()
    {
        PLOG_DEBUG << "~Timer destroyed";
        timer_.cancel();
    }

    void stop()
    {
        timer_.cancel();
    }

    void bindTimerFinish(TimerFinishCallback cb)
    {
        timer_finish_cb_ = cb;
    }

    void start(uint32_t ms)
    {
        timer_.expires_after(std::chrono::milliseconds(ms));
        timer_.async_wait(
            [self = shared_from_this()]
            (const asio::error_code& ec)
            {
                if (ec) {
                    return;
                }

                PLOG_DEBUG << "tick\n";

                if(self->timer_finish_cb_)
                {
                    self->timer_finish_cb_();
                }
            });
    }

private:
    asio::steady_timer timer_;
    TimerFinishCallback timer_finish_cb_;
};
