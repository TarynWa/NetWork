#include "EventLoopThread.hpp"

namespace nwl {

EventLoopThread::EventLoopThread(ThreadInitCallback cb)
    : initCb_(std::move(cb)) {}

EventLoopThread::~EventLoopThread() {
    {
        // 与 threadFunc 收尾互斥，防止读取悬垂 loop_
        std::lock_guard<std::mutex> lock(mutex_);
        if (loop_ != nullptr) {
            loop_->quit();
        }
    }
    if (thread_.joinable()) {
        thread_.join();
    }
}

EventLoop* EventLoopThread::startLoop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        thread_ = std::thread([this] { threadFunc(); });
    }
    // 等待 threadFunc 把 loop_ 指针放出来
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return loop_ != nullptr; });
    }
    return loop_;
}

void EventLoopThread::threadFunc() {
    EventLoop loop;                    // 在 IO 线程内构造：满足 one-loop-per-thread

    if (initCb_) {
        initCb_(&loop);
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;
        cond_.notify_one();
    }
    loop.loop();                       // 阻塞至 quit
    // 防御：loop 退出后清空指针（此对象析构时还会再走一遍 quit 分支判断）
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;
}

} // namespace nwl
