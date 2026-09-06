#ifndef NWL_EVENTLOOP_THREAD_POOL_HPP
#define NWL_EVENTLOOP_THREAD_POOL_HPP
// EventLoopThreadPool：N 个 IO 线程池，round-robin 分发连接（plan.md §3.9）
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "nwl/EventLoop.hpp"

namespace nwl {

class EventLoopThread;

class EventLoopThreadPool {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    EventLoopThreadPool(EventLoop* baseLoop, std::string name);
    ~EventLoopThreadPool();

    void setThreadNum(int n) { numThreads_ = static_cast<size_t>(n); }
    int threadNum() const { return static_cast<int>(numThreads_); }

    /// 创建全部 IO 线程；必须在主线程调用一次
    void start(ThreadInitCallback cb = nullptr);

    /// round-robin 取下一个 IO loop；未启动多线程时返回 baseLoop
    EventLoop* getNextLoop();

private:
    EventLoop* baseLoop_;
    const std::string name_;
    bool started_ = false;
    size_t numThreads_ = 0;                // 目标 IO 线程数
    size_t next_ = 0;
    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;        // 与 threads_ 一一对应
};

} // namespace nwl

#endif // NWL_EVENTLOOP_THREAD_POOL_HPP
