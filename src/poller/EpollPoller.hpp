#ifndef NWL_EPOLL_POLLER_HPP
#define NWL_EPOLL_POLLER_HPP
// EpollPoller：epoll（默认 LT 模式）封装，Linux 平台默认 Poller 后端（plan.md §3.3）
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>
#include "nwl/Poller.hpp"

namespace nwl {

class EpollPoller : public Poller {
public:
    explicit EpollPoller(EventLoop* loop);
    ~EpollPoller() override;

    Timestamp poll(int timeoutMs, ChannelList* activeChannels) override;
    void updateChannel(Channel* channel) override;
    void removeChannel(Channel* channel) override;

private:
    static constexpr int kInitEventListSize = 16;   // events_ 初始容量，按需翻倍
    static constexpr int kMaxEventListSize  = 1024;

    static const int kNew     = -1;   // 尚未加入 channels_ 表
    static const int kAdded   = 1;    // 已在 epoll 关注中
    static const int kDeleted = 2;    // 已从 epoll 移除但仍在表中

    void fillActiveChannels(int numEvents, ChannelList* activeChannels) const;
    /// 统一封装 epoll_ctl(ADD/MOD/DEL)
    void update(int operation, Channel* channel);

    int epollfd_;                       // ::epoll_create1(EPOLL_CLOEXEC)
    std::vector<struct epoll_event> events_;
    std::unordered_map<int, Channel*> channels_;   // fd → Channel（由派生类自持，muduo 同构）
};

} // namespace nwl

#endif // NWL_EPOLL_POLLER_HPP
