#include "nwl/Channel.hpp"
#include "nwl/EventLoop.hpp"
#include <sys/epoll.h>

namespace nwl {

const uint32_t Channel::kNone;
const uint32_t Channel::kRead;
const uint32_t Channel::kWrite;

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop), fd_(fd) {}

void Channel::tie(const std::shared_ptr<void>& owner) {
    tie_ = owner;
    tied_ = true;
}

void Channel::handleEvent(Timestamp receiveTime) {
    std::shared_ptr<void> guard;
    if (tied_) {
        guard = tie_.lock();
        if (!guard) return;          // 宿主已析构，本轮事件静默丢弃
    }
    handleEventWithGuard(receiveTime);
}

void Channel::update() {
    loop_->updateChannel(this);
}

void Channel::remove() {
    loop_->removeChannel(this);
}

void Channel::handleEventWithGuard(Timestamp receiveTime) {
    eventHandling_ = true;
    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
        if (closeCallback_) closeCallback_();
    }
    if (revents_ & EPOLLERR) {
        if (errorCallback_) errorCallback_();
    }
    if (revents_ & (EPOLLIN | EPOLLPRI)) {
        if (readCallback_) readCallback_(receiveTime);
    }
    if (revents_ & EPOLLOUT) {
        if (writeCallback_) writeCallback_();
    }
    eventHandling_ = false;
}

} // namespace nwl
