#ifndef NWL_TIMERQUEUE_HPP
#define NWL_TIMERQUEUE_HPP
// TimerQueue：基于 timerfd + 有序容器的定时器（内部组件，经 EventLoop::runAt 暴露）
// 取消语义：可取消任意未触发/周期性任务；回调执行期间 cancel 同样安全
// （实现上：到期集合先摘除再逐个触发，fire 前复查 canceled 集合，plan.md §3.5 的简化等价变体）
#include <sys/timerfd.h>
#include <memory>
#include <set>
#include <unordered_set>
#include <vector>
#include "nwl/Callbacks.hpp"
#include "nwl/Channel.hpp"
#include "nwl/TimerId.hpp"

namespace nwl {

class EventLoop;

class Timer {
public:
    Timer(TimerCallback cb, Timestamp when, double intervalSec, int64_t seq)
        : callback_(std::move(cb)),
          expirationUs_(when.getmicro()),
          intervalUs_(static_cast<uint64_t>(intervalSec * 1000 * 1000)),
          repeat_(intervalSec > 0.0),
          sequence_(seq) {}

    void run() const { callback_(); }

    /// 周期任务重排到下一轮；一次性任务返回 false
    bool restart(Timestamp now) {
        if (repeat_) {
            expirationUs_ = now.getmicro() + intervalUs_;
            return true;
        }
        return false;
    }

    Timestamp expiration() const { return Timestamp(expirationUs_); }
    int64_t   sequence() const   { return sequence_; }
    bool      valid() const      { return expirationUs_ > 0; }

private:
    TimerCallback callback_;
    uint64_t expirationUs_;
    const uint64_t intervalUs_;
    const bool repeat_;
    const int64_t sequence_;
};

class TimerQueue {
public:
    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    /// 线程安全：跨线程自动转投所属 loop
    TimerId addTimer(TimerCallback cb, Timestamp when, double intervalSeconds);
    void cancel(TimerId id);

private:
    using Entry = std::pair<Timestamp, Timer*>;
    using ActiveTimer = std::pair<Timer*, int64_t>;   // 用 sequence 区分同刻同址

    void addTimerInLoop(Timer* timer);
    void cancelInLoop(TimerId id);

    void handleRead();                                // timerfd 到期

    std::vector<Entry> getExpired(Timestamp now);     // 摘取 ≤now 的全部到期项
    void resetTimerFd(Timestamp when);                // 重设最近一次到期时刻
    void reset(const std::vector<Entry>& expired, Timestamp now);

    EventLoop* loop_;
    const int timerfd_;
    std::unique_ptr<Channel> timerfdChannel_;

    std::set<Entry> timers_;                          // 主容器：按 (到期时刻, 指针) 全序
    std::unordered_map<int64_t, Timer*> activeTimers_; // seq → Timer，支持 O(log) cancel
    std::unordered_set<int64_t> canceledWhilePending_; // 已摘出、待 fire 时复查跳过
};

} // namespace nwl

#endif // NWL_TIMERQUEUE_HPP
