#ifndef NWL_TIMERID_HPP
#define NWL_TIMERID_HPP
// TimerId：不透明定时器句柄（用于 cancel），由 TimerQueue 创建与解释
#include <cstdint>

namespace nwl {

class Timer;

class TimerId {
public:
    TimerId() : timer_(nullptr), sequence_(0) {}
    friend class TimerQueue;    // 仅允许队列内部构造/比较

private:
    explicit TimerId(Timer* timer, int64_t seq)
        : timer_(timer), sequence_(seq) {}

    Timer* timer_;
    int64_t sequence_;
};

} // namespace nwl

#endif // NWL_TIMERID_HPP
