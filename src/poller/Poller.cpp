#include "nwl/Poller.hpp"
#include "EpollPoller.hpp"

namespace nwl {

Poller::Poller(EventLoop* loop)
    : ownerLoop_(loop) {}

Poller* Poller::newDefaultPoller(EventLoop* loop) {
    // 方案一 Linux 后端默认 epoll；poll/select 降级实现按 plan.md M4+ 增量接入
    return new EpollPoller(loop);
}

} // namespace nwl
