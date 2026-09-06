#ifndef NWL_CHANNEL_HPP
#define NWL_CHANNEL_HPP
// Channel：fd 事件绑定器（一个 Channel 负责一个 fd 的一类事件分发）
// 所属 EventLoop 的 IO 线程独占其状态，跨线程修改必须经 runInLoop/queueInLoop。
#include <sys/epoll.h>
#include <functional>
#include <memory>
#include "nwl/Callbacks.hpp"

namespace nwl {

class EventLoop;

class Channel {
public:
    using ReadEventCallback = std::function<void(Timestamp)>;
    using EventCallback     = Functor;

    // 关注事件类型（与 epoll 位图一致）
    static const uint32_t kNone  = 0;
    static const uint32_t kRead  = EPOLLIN | EPOLLPRI;
    static const uint32_t kWrite = EPOLLOUT;

    Channel(EventLoop* loop, int fd);
    ~Channel() = default;
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    void handleEvent(Timestamp receiveTime);

    void setReadCallback(ReadEventCallback cb)   { readCallback_   = std::move(cb); }
    void setWriteCallback(EventCallback cb)      { writeCallback_  = std::move(cb); }
    void setCloseCallback(EventCallback cb)      { closeCallback_  = std::move(cb); }
    void setErrorCallback(EventCallback cb)      { errorCallback_  = std::move(cb); }

    /// 绑定宿主（TcpConnection）弱引用：防止事件到达时宿主已析构（UAF 第一防线，plan.md §3.4）
    void tie(const std::shared_ptr<void>& owner);

    int      fd() const          { return fd_; }
    uint32_t events() const      { return events_; }
    void     setRevents(uint32_t rvt) { revents_ = rvt; }      // 仅 Poller 派发时调用
    bool     isNoneEvent() const { return events_ == kNone; }

    void enableReading()  { events_ |= kRead;  update(); }
    void enableWriting()  { events_ |= kWrite; update(); }
    void disableWriting() { events_ &= ~kWrite; update(); }
    void disableAll()     { events_ = kNone;   update(); }

    bool isWriting() const { return events_ & kWrite; }
    bool isReading() const { return events_ & kRead; }

    // Poller 内部状态标记（kNew/kAdded/kDeleted），由 Poller 维护
    int  index() const        { return index_; }
    void setIndex(int idx)    { index_ = idx; }

    EventLoop* ownerLoop() const { return loop_; }
    void remove();

private:
    void update();
    void handleEventWithGuard(Timestamp receiveTime);

    EventLoop* loop_;
    const int  fd_;
    uint32_t   events_  = kNone;
    uint32_t   revents_ = kNone;
    int        index_   = -1;            // kNew

    std::weak_ptr<void> tie_;
    bool tied_ = false;

    bool eventHandling_ = false;

    ReadEventCallback readCallback_;
    EventCallback     writeCallback_;
    EventCallback     closeCallback_;
    EventCallback     errorCallback_;
};

} // namespace nwl

#endif // NWL_CHANNEL_HPP
