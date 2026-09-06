#ifndef NWL_POLLER_HPP
#define NWL_POLLER_HPP
// Poller：IO 多路复用统一抽象（方案一当前仅实现 EpollPoller；
// 接口层为后续 SelectPoller/PollPoller 降级与 ET 实验分支预留，见 plan.md §3.3）
#include <sys/time.h>
#include <vector>
#include "nwl/Callbacks.hpp"

namespace nwl {

class Channel;
class EventLoop;

class Poller {
public:
    using ChannelList = std::vector<Channel*>;

    explicit Poller(EventLoop* loop);
    virtual ~Poller() = default;
    Poller(const Poller&) = delete;
    Poller& operator=(const Poller&) = delete;

    /// 轮询就绪事件并回填 activeChannels；返回本次轮询时刻
    virtual Timestamp poll(int timeoutMs, ChannelList* activeChannels) = 0;
    /// 新增/修改 Channel 关注的事件
    virtual void updateChannel(Channel* channel) = 0;
    /// 移除 Channel（要求其已处于 none-event 且登记在表）
    virtual void removeChannel(Channel* channel) = 0;

    /// 工厂方法：按平台能力返回默认 Poller 实现（Linux → EpollPoller）
    static Poller* newDefaultPoller(EventLoop* loop);

protected:
    EventLoop* ownerLoop_;
};

} // namespace nwl

#endif // NWL_POLLER_HPP
