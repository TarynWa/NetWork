#include "EventLoopThreadPool.hpp"
#include <cassert>
#include "Logger.hpp"
#include "EventLoopThread.hpp"

namespace nwl {

EventLoopThreadPool::EventLoopThreadPool(EventLoop* baseLoop, std::string name)
    : baseLoop_(baseLoop), name_(std::move(name)) {}

EventLoopThreadPool::~EventLoopThreadPool() {
    // 线程与 loop 的清理由 EventLoopThread 析构统一接管（quit + join）
}

void EventLoopThreadPool::start(ThreadInitCallback cb) {
    assert(baseLoop_->isInLoopThread());
    started_ = true;
    for (size_t i = 0; i < numThreads_; ++i) {
        threads_.push_back(std::make_unique<EventLoopThread>(cb));
        loops_.push_back(threads_.back()->startLoop());
        WT_LOG_INFO << "EventLoopThread #" << i << " of pool '" << name_ << "' started";
    }
}

EventLoop* EventLoopThreadPool::getNextLoop() {
    assert(baseLoop_->isInLoopThread());
    EventLoop* loop = baseLoop_;
    if (!loops_.empty()) {
        loop = loops_[next_];
        next_ = (next_ + 1) % loops_.size();
    }
    return loop;
}

} // namespace nwl
