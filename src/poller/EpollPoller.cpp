#include "EpollPoller.hpp"
#include <sys/epoll.h>
#include <unistd.h>
#include <cerrno>
#include <cassert>
#include "Logger.hpp"      // WT_LOG_* 日志桥接（lib::muduo_log）
#include "nwl/Channel.hpp"
#include "nwl/EventLoop.hpp"

namespace nwl {

const int EpollPoller::kNew;
const int EpollPoller::kAdded;
const int EpollPoller::kDeleted;

namespace {
const char* operationToString(int op) {
    switch (op) {
        case EPOLL_CTL_ADD: return "ADD";
        case EPOLL_CTL_DEL: return "DEL";
        case EPOLL_CTL_MOD: return "MOD";
        default: return "Unknown Operation";
    }
}
} // anonymous namespace

EpollPoller::EpollPoller(EventLoop* loop)
    : Poller(loop),
      epollfd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(kInitEventListSize) {
    if (epollfd_ < 0) {
        WT_LOG_FATAL << "EpollPoller::EpollPoller epoll_create1 failed, errno=" << errno;
        ::abort();
    }
}

EpollPoller::~EpollPoller() {
    if (epollfd_ >= 0) {
        ::close(epollfd_);
    }
}

Timestamp EpollPoller::poll(int timeoutMs, ChannelList* activeChannels) {
    int numEvents = ::epoll_wait(epollfd_,
                                 &*events_.begin(),
                                 static_cast<int>(events_.size()),
                                 timeoutMs);
    Timestamp now(Timestamp::Now());
    if (numEvents > 0) {
        fillActiveChannels(numEvents, activeChannels);
        // 恰好用满则翻倍扩容，封顶 kMaxEventListSize
        if (numEvents == static_cast<int>(events_.size())
            && static_cast<int>(events_.size()) < kMaxEventListSize) {
            events_.resize(events_.size() * 2);
        }
    } else if (numEvents == 0) {
        WT_LOG_DEBUG << "nothing happened";
    } else if (errno != EINTR) {                 // 被信号打断属正常，静默重试
        WT_LOG_ERROR << "EpollPoller::poll() errno=" << errno;
    }
    return now;
}

void EpollPoller::fillActiveChannels(int numEvents,
                                     ChannelList* activeChannels) const {
    for (int i = 0; i < numEvents; ++i) {
        auto* channel = static_cast<Channel*>(events_[i].data.ptr);
        channel->setRevents(static_cast<uint32_t>(events_[i].events));
        activeChannels->push_back(channel);
    }
}

void EpollPoller::updateChannel(Channel* channel) {
    ownerLoop_->assertInLoopThread();
    const int idx = channel->index();
    if (idx == kNew || idx == kDeleted) {
        // 全新 / 曾被删除后重新挂载：登记映射并 ADD
        int fd = channel->fd();
        if (idx == kNew) {
            channels_[fd] = channel;
        }
        channel->setIndex(kAdded);
        update(EPOLL_CTL_ADD, channel);
    } else {                                       // idx == kAdded
        if (channel->isNoneEvent()) {
            update(EPOLL_CTL_DEL, channel);
            channel->setIndex(kDeleted);           // 保留映射，等待 removeChannel 清理
        } else {
            update(EPOLL_CTL_MOD, channel);        // 关注集变化（enable/disableWriting 等）
        }
    }
}

void EpollPoller::removeChannel(Channel* channel) {
    ownerLoop_->assertInLoopThread();
    int fd = channel->fd();
    // 仅允许移除“已不关注任何事件且登记在表”的 Channel
    assert(channels_.find(fd) != channels_.end());
    assert(channel->isNoneEvent());
    if (channel->index() == kAdded) {
        update(EPOLL_CTL_DEL, channel);
    }
    channels_.erase(fd);
    channel->setIndex(kNew);
}

void EpollPoller::update(int operation, Channel* channel) {
    struct epoll_event ev{};
    ev.events   = static_cast<uint32_t>(channel->events());
    ev.data.ptr = channel;                          // O(1) 由事件反查 Channel
    int fd      = channel->fd();
    if (::epoll_ctl(epollfd_, operation, fd, &ev) < 0) {
        WT_LOG_ERROR << "epoll_ctl op=" << operationToString(operation)
                     << " fd=" << fd << " errno=" << errno;
    }
}

} // namespace nwl
