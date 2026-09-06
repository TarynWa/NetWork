#ifndef NWL_EVENTLOOP_HPP
#define NWL_EVENTLOOP_HPP
// EventLoop：One-Loop-Per-Thread Reactor 核心骨架（plan.md §3.2 / §5.2）
// 铁律：loop()/poller 仅由所属 IO 线程驱动；外部线程只能经 runInLoop/queueInLoop 投递。
#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include "nwl/Channel.hpp"
#include "nwl/Poller.hpp"
#include "nwl/TimerId.hpp"

namespace nwl {

class EventLoop;
class TimerQueue;

class EventLoop {
public:
    using ChannelList = Poller::ChannelList;

    EventLoop();
    ~EventLoop();
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    /// 事件循环主入口：poll → 派发 activeChannels → 执行 pendingFunctors
    void loop();
    /// 跨线程请求退出（内部唤醒 epoll_wait）
    void quit();

    /// 本线程直接执行，否则转投所属 loop
    void runInLoop(Functor f);
    /// 入队并在下一轮迭代（或立即唤醒）时执行；本线程调用也会写入 eventfd，
    /// 防止此刻正阻塞在 doPendingFunctors 执行阶段而丢唤醒
    void queueInLoop(Functor f);
    /// 向自身 eventfd 写 8 字节以打断 epoll_wait
    void wakeup();

    // ---- 定时器（TimerQueue 集成，线程安全）----
    TimerId runAt(TimerCallback cb, Timestamp when);
    TimerId runAfter(TimerCallback cb, double delaySeconds);
    /// intervalSeconds <= 0 视为一次性任务
    TimerId runEvery(TimerCallback cb, double intervalSeconds);
    void cancelTimer(TimerId id);

    // 由 Channel 触发的事件表维护（要求在所属 loop 线程）
    void updateChannel(Channel* channel);
    void removeChannel(Channel* channel);

    bool isInLoopThread() const { return ownerId_ == std::this_thread::get_id(); }
    void assertInLoopThread();
    /// 返回当前线程已运行的 EventLoop（无则 nullptr）
    static EventLoop* getEventLoopOfCurrentThread();

private:
    void abortNotInLoopThread() __attribute__((noreturn));
    void handleWakeupRead();      // 读走 eventfd 唤醒计数
    void doPendingFunctors();     // 锁内 swap、锁外执行（防死锁关键）

    static constexpr int kPollTimeMs = 10000;   // 被 wakeup/quit 打断

    std::atomic<bool> looping_{false};
    std::atomic<bool> quit_{false};
    std::atomic<bool> callingPendingFunctors_{false};
    int64_t iteration_ = 0;
    Timestamp pollReturnTime_;

    const std::thread::id ownerId_;
    static thread_local EventLoop* t_loopInThisThread;

    std::unique_ptr<Poller> poller_;
    std::unique_ptr<TimerQueue> timers_;
    int wakeupFd_;
    std::unique_ptr<Channel> wakeupChannel_;

    ChannelList activeChannels_;

    mutable std::mutex functorMutex_;
    std::vector<Functor> pendingFunctors_;
};

} // namespace nwl

#endif // NWL_EVENTLOOP_HPP
