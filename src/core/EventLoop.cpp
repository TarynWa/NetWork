#include "nwl/EventLoop.hpp"
#include <sys/eventfd.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include "Logger.hpp"      // WT_LOG_* 日志桥接（lib::muduo_log）
#include "TimerQueue.hpp"

namespace nwl {

thread_local EventLoop* EventLoop::t_loopInThisThread = nullptr;

namespace {
const int kWakeupWriteBytes = 8;        // eventfd 计数单位
}

EventLoop::EventLoop()
    : ownerId_(std::this_thread::get_id()),
      poller_(Poller::newDefaultPoller(this)),
      timers_(new TimerQueue(this)),
      wakeupFd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      wakeupChannel_(nullptr) {
    if (t_loopInThisThread != nullptr) {
        // 一个线程同一时刻只允许运行一个 EventLoop（muduo 语义）
        WT_LOG_FATAL << "Another EventLoop " << static_cast<const void*>(t_loopInThisThread)
                     << " already exists in this thread";
        ::abort();
    }
    if (wakeupFd_ < 0) {
        WT_LOG_FATAL << "EventLoop::EventLoop eventfd failed, errno=" << errno;
        ::abort();
    }
    t_loopInThisThread = this;
    wakeupChannel_.reset(new Channel(this, wakeupFd_));
    wakeupChannel_->setReadCallback([this](Timestamp) { handleWakeupRead(); });
    // 关注可读但不作 wakeup 用途的唯一通道，允许常挂 poll 不产生 EPOLLHUP 噪声
    wakeupChannel_->enableReading();
    WT_LOG_INFO << "EventLoop created " << static_cast<const void*>(this);
}

EventLoop::~EventLoop() {
    // 析构要求在所属 loop 线程执行（与 muduo 一致）
    assertInLoopThread();
    wakeupChannel_->disableAll();
    wakeupChannel_->remove();
    wakeupChannel_.reset();
    if (wakeupFd_ >= 0) {
        ::close(wakeupFd_);
    }
    t_loopInThisThread = nullptr;
    WT_LOG_INFO << "EventLoop " << static_cast<const void*>(this) << " destructed";
}

void EventLoop::loop() {
    assertInLoopThread();
    looping_.store(true);
    quit_.store(false);
    WT_LOG_INFO << "EventLoop " << static_cast<const void*>(this)
                << " start looping in thread " << ownerId_;
    while (!quit_.load()) {
        activeChannels_.clear();
        pollReturnTime_ = poller_->poll(kPollTimeMs, &activeChannels_);
        ++iteration_;
        for (Channel* ch : activeChannels_) {
            ch->handleEvent(pollReturnTime_);
        }
        doPendingFunctors();
    }
    looping_.store(false);
    WT_LOG_INFO << "EventLoop " << static_cast<const void*>(this)
                << " stop looping after " << iteration_ << " iterations";
}

void EventLoop::quit() {
    quit_.store(true);
    if (!isInLoopThread()) {
        wakeup();                          // 打断阻塞中的 epoll_wait
    }
}

void EventLoop::runInLoop(Functor f) {
    if (isInLoopThread()) {
        f();
    } else {
        queueInLoop(std::move(f));
    }
}

void EventLoop::queueInLoop(Functor f) {
    {
        std::lock_guard<std::mutex> lock(functorMutex_);
        pendingFunctors_.push_back(std::move(f));
    }
    // 两个条件缺一不可：本线程调用但正在执行 pending 批次时，
    // epoll_wait 已被跳过、不会被新回调“顺带”处理——必须主动唤醒
    if (!isInLoopThread() || callingPendingFunctors_.load()) {
        wakeup();
    }
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(wakeupFd_, &one, sizeof one);
    if (n != sizeof one) {
        WT_LOG_ERROR << "EventLoop::wakeup() writes " << n << " bytes instead of 8";
    }
}

TimerId EventLoop::runAt(TimerCallback cb, Timestamp when) {
    return timers_->addTimer(std::move(cb), when, 0.0);
}

TimerId EventLoop::runAfter(TimerCallback cb, double delaySeconds) {
    const auto delayUs = static_cast<uint64_t>(delaySeconds * 1000 * 1000);
    return timers_->addTimer(std::move(cb),
                             Timestamp(Timestamp::Now().getmicro() + delayUs), 0.0);
}

TimerId EventLoop::runEvery(TimerCallback cb, double intervalSeconds) {
    const auto intervalUs = static_cast<uint64_t>(intervalSeconds * 1000 * 1000);
    return timers_->addTimer(std::move(cb),
                             Timestamp(Timestamp::Now().getmicro() + intervalUs),
                             intervalSeconds);
}

void EventLoop::cancelTimer(TimerId id) {
    timers_->cancel(id);
}

void EventLoop::handleWakeupRead() {
    uint64_t one = 1;
    ssize_t n = ::read(wakeupFd_, &one, sizeof one);   // EFD_NONBLOCK 下 EAGAIN 安全
    if (n != sizeof one && errno != EAGAIN) {
        WT_LOG_ERROR << "EventLoop::handleWakeupRead reads " << n << " bytes";
    }
}

void EventLoop::updateChannel(Channel* channel) {
    assertInLoopThread();
    poller_->updateChannel(channel);
}

void EventLoop::removeChannel(Channel* channel) {
    assertInLoopThread();
    poller_->removeChannel(channel);
}

void EventLoop::assertInLoopThread() {
    if (!isInLoopThread()) {
        abortNotInLoopThread();
    }
}

EventLoop* EventLoop::getEventLoopOfCurrentThread() {
    return t_loopInThisThread;
}

void EventLoop::abortNotInLoopThread() {
    WT_LOG_ERROR << "EventLoop " << static_cast<const void*>(this)
                 << " was created in thread " << ownerId_
                 << ", but accessed from thread " << std::this_thread::get_id();
    ::abort();
}

void EventLoop::doPendingFunctors() {
    std::vector<Functor> functors;
    callingPendingFunctors_.store(true);
    {   // 锁内仅换出队列——回调内再调 queueInLoop 不会死锁（swap-out-then-run）
        std::lock_guard<std::mutex> lock(functorMutex_);
        functors.swap(pendingFunctors_);
    }
    for (Functor& functor : functors) {
        functor();
    }
    callingPendingFunctors_.store(false);
}

} // namespace nwl
