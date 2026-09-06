#include "TimerQueue.hpp"
#include <sys/timerfd.h>
#include <unistd.h>
#include <cerrno>
#include <cstdint>
#include <atomic>
#include "Logger.hpp"
#include "nwl/EventLoop.hpp"

namespace nwl {

namespace {
std::atomic<int64_t> s_numCreated{0};

// 距目标的剩余微秒（下限 100µs，无符号下溢自然钳位）
std::uint64_t microsFromNow(Timestamp when) {
    Timestamp now(Timestamp::Now());
    std::uint64_t diff = when.getmicro() - now.getmicro();
    return diff < 100 ? 100 : diff;
}

struct timespec toTimespec(std::uint64_t micros) {
    struct timespec ts{};
    ts.tv_sec  = static_cast<time_t>(micros / 1000000);
    ts.tv_nsec = static_cast<long>((micros % 1000000) * 1000);
    return ts;
}
} // namespace

TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop),
      timerfd_(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)),
      timerfdChannel_(),
      timers_(),
      activeTimers_(),
      canceledWhilePending_() {
    if (timerfd_ < 0) {
        WT_LOG_FATAL << "TimerQueue::TimerQueue timerfd_create failed errno=" << errno;
        ::abort();
    }
    timerfdChannel_.reset(new Channel(loop, timerfd_));
    timerfdChannel_->setReadCallback([this](Timestamp) { handleRead(); });
}

TimerQueue::~TimerQueue() {
    timerfdChannel_->disableAll();
    timerfdChannel_->remove();
    ::close(timerfd_);
    for (const Entry& e : timers_) {
        delete e.second;
    }
}

TimerId TimerQueue::addTimer(TimerCallback cb, Timestamp when, double intervalSeconds) {
    auto* timer = new Timer(std::move(cb), when, intervalSeconds,
                            ++s_numCreated);
    loop_->runInLoop([this, timer] { addTimerInLoop(timer); });   // 跨线程转投
    return TimerId(timer, timer->sequence());
}

void TimerQueue::addTimerInLoop(Timer* timer) {
    loop_->assertInLoopThread();
    Timestamp when = timer->expiration();
    const bool earliestChanged =
        timers_.empty() || when < timers_.begin()->first;
    {
        auto r = timers_.emplace(when, timer);
        if (!r.second) {   // 同刻同指针理论不可能；防御性兜底
            WT_LOG_ERROR << "duplicated timer entry seq=" << timer->sequence();
            delete timer;
            return;
        }
    }
    activeTimers_[timer->sequence()] = timer;

    // 首个任务挂载 channel；此后常驻（空表时 disarm timerfd 静音）
    if (!timerfdChannel_->isReading()) {
        timerfdChannel_->enableReading();
    }
    if (earliestChanged) {
        resetTimerFd(when);
    }
}

void TimerQueue::cancel(TimerId id) {
    loop_->runInLoop([this, id] { cancelInLoop(id); });
}

void TimerQueue::cancelInLoop(TimerId id) {
    loop_->assertInLoopThread();
    auto it = activeTimers_.find(id.sequence_);
    if (it != activeTimers_.end()) {
        // 未到期：双容器同步摘除并立即释放
        timers_.erase(std::make_pair(it->second->expiration(), it->second));
        delete it->second;
        activeTimers_.erase(it);
    } else {
        // 已摘入本轮待触发集合：登记序号，fire 前复查跳过
        // （对一次性已触发/未知 id 同样安全：fire 后集合会被清查）
        canceledWhilePending_.insert(id.sequence_);
    }
}

void TimerQueue::handleRead() {
    loop_->assertInLoopThread();
    uint64_t howmany = 0;
    ssize_t n = ::read(timerfd_, &howmany, sizeof howmany);
    (void)n;

    const Timestamp now(Timestamp::Now());
    std::vector<Entry> expired = getExpired(now);

    // 先摘除后触发：回调内 addTimer/cancel 均安全
    for (const Entry& e : expired) {
        Timer* t = e.second;
        if (canceledWhilePending_.erase(t->sequence()) > 0) {
            continue;                    // 已被 cancel，回调不再执行
        }
        t->run();
    }

    // 统一收尾：周期任务重排；其余按所有权规则释放
    for (const Entry& e : expired) {
        Timer* t = e.second;
        bool ownerReleaseDone = false;
        if (t->valid() && t->restart(now)) {
            activeTimers_[t->sequence()] = t;             // 回到活动集
            auto inserted = timers_.emplace(t->expiration(), t);
            if (!inserted.second) {
                // 极端并发窗口防御：同为 active 时以事件驱动的插入为准
                delete t;
                ownerReleaseDone = true;
                activeTimers_.erase(t->sequence());
            } else {
                ownerReleaseDone = true;
            }
        }
        if (!ownerReleaseDone) {
            // 一次性任务：fire 后由队列独占销毁
            // 若它已在回调里被外部经 activeTimers_ 命中取消则不会走到这里
            // （getExpired 摘除时同步从 activeTimers_ 移除）
            delete t;
        }
    }

    // 空表静音，否则聚焦最近一次到期
    struct itimerspec spec{};
    if (!timers_.empty()) {
        spec.it_value = toTimespec(microsFromNow(timers_.begin()->first));
    }
    ::timerfd_settime(timerfd_, 0, &spec, nullptr);

    canceledWhilePending_.clear();   // 本轮全部复核完毕
}

std::vector<TimerQueue::Entry> TimerQueue::getExpired(Timestamp now) {
    std::vector<Entry> expired;
    // sentinel：全序集合中定位首个 >now 的项
    Entry sentry(now, reinterpret_cast<Timer*>(~static_cast<uintptr_t>(0)));
    auto endIt = timers_.lower_bound(sentry);
    if (endIt == timers_.end() || !(endIt->first <= now)) {
        // lower_bound 返回首个 >= sentry 的位置；
        // 注意 Timestamp 全序比较，直接回退收集 [begin, endIt)
    }
    for (auto it = timers_.begin(); it != endIt; ++it) {
        expired.push_back(*it);
        activeTimers_.erase(it->second->sequence());  // 所有权转移给调用方局部向量
    }
    timers_.erase(timers_.begin(), endIt);
    return expired;
}

void TimerQueue::resetTimerFd(Timestamp when) {
    struct itimerspec spec{};
    spec.it_value = toTimespec(microsFromNow(when));
    if (::timerfd_settime(timerfd_, 0, &spec, nullptr) < 0) {
        WT_LOG_ERROR << "TimerQueue::resetTimerFd errno=" << errno;
    }
}

void TimerQueue::reset(const std::vector<Entry>& expired, Timestamp now) {
    // 兼容 plan.md 设计签名保留；实际重排逻辑并入 handleRead 收尾阶段
    (void)expired;
    (void)now;
}

} // namespace nwl
