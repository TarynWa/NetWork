#ifndef NWL_ACCEPTOR_HPP
#define NWL_ACCEPTOR_HPP
// Acceptor：监听 socket 封装（内部组件）。EMFILE 用 idleFd 借位兜底（plan.md §3.8）
#include <fcntl.h>
#include <memory>
#include "nwl/Channel.hpp"
#include "nwl/InetAddress.hpp"
#include "nwl/Socket.hpp"

namespace nwl {

class EventLoop;

class Acceptor {
public:
    using NewConnectionCallback =
        std::function<void(int sockfd, const InetAddress& peer)>;

    Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reusePort);
    ~Acceptor();

    void setNewConnectionCallback(NewConnectionCallback cb) { newConnectionCallback_ = std::move(cb); }
    bool listening() const { return listening_; }
    void listen();          // 由 TcpServer::start 调用

private:
    void handleRead();      // epoll LT 下常驻可读：accept 惊群风险有限

    EventLoop* loop_;
    Socket acceptSocket_;
    Channel acceptChannel_;
    NewConnectionCallback newConnectionCallback_;
    int idleFd_;            // /dev/null 预留席位，专治 fd 表满时的 EMFILE 死循环
    bool listening_ = false;
};

} // namespace nwl

#endif // NWL_ACCEPTOR_HPP
