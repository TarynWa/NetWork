#ifndef NWL_EVENTLOOP_THREAD_HPP
#define NWL_EVENTLOOP_THREAD_HPP
// EventLoopThread：线程与 EventLoop 的一对一绑定（内部组件）
// startLoop 返回时保证该线程已创建 loop 并即将进入 poll（promise 栅栏，plan.md R7）
#include <condition_variable>
#include <memory>
#include <thread>
#include "nwl/EventLoop.hpp"

namespace nwl {

class EventLoopThread {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    explicit EventLoopThread(ThreadInitCallback cb = nullptr);
    ~EventLoopThread();               // 析构触发 loop 退出并 join

    /// 启动 IO 线程；阻塞直至其 EventLoop 就绪并返回指针（线程内对象，勿手动 delete）
    EventLoop* startLoop();

private:
    void threadFunc();

    EventLoop* loop_ = nullptr;       // 指向 threadFunc 栈上的 loop
    bool exiting_ = false;
    ThreadInitCallback initCb_;

    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cond_;
};

} // namespace nwl

#endif // NWL_EVENTLOOP_THREAD_HPP
