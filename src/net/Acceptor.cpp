#include "Acceptor.hpp"
#include <unistd.h>
#include <cassert>
#include <cerrno>
#include "Logger.hpp"
#include "../util/SocketsOps.hpp"
#include "nwl/EventLoop.hpp"

namespace nwl {

Acceptor::Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reusePort)
    : loop_(loop),
      acceptSocket_(listenAddr.family()),
      acceptChannel_(loop, acceptSocket_.fd()),
      idleFd_(::open("/dev/null", O_RDONLY | O_CLOEXEC)) {
    assert(idleFd_ >= 0);
    acceptSocket_.setReuseAddr(true);
    if (reusePort) {
        acceptSocket_.setReusePort(true);
    }
    acceptSocket_.bindAddress(listenAddr);
    acceptChannel_.setReadCallback([this](Timestamp) { handleRead(); });
}

Acceptor::~Acceptor() {
    acceptChannel_.disableAll();
    acceptChannel_.remove();
    if (idleFd_ >= 0) {
        ::close(idleFd_);
    }
}

void Acceptor::listen() {
    listening_ = true;
    acceptSocket_.listen();
    acceptChannel_.enableReading();
    WT_LOG_INFO << "Acceptor listening on "
                << acceptSocket_.getLocalAddr().toIpPort();
}

void Acceptor::handleRead() {
    InetAddress peer;
    int connfd = acceptSocket_.accept(&peer);
    if (connfd >= 0) {
        if (newConnectionCallback_) {
            newConnectionCallback_(connfd, peer);
        } else {
            sockets::close(connfd);          // 无回调时礼貌关闭
        }
        return;
    }

    const int savedErrno = errno;
    switch (savedErrno) {
        case EAGAIN:
#if EAGAIN != EWOULDBLOCK
        case EWOULDBLOCK:
#endif
        case ECONNABORTED:
        case EINTR:
            return;                           // 常规瞬态，静默重试
        case EMFILE: {
            // fd 表满：借用 idleFd 的席位把这根连接吃掉再归还，
            // 避免_LT 模式下“永远可读→空转”风暴（plan.md §3.8）
            ::close(idleFd_);
            int wasted = ::accept(acceptSocket_.fd(), nullptr, nullptr);
            if (wasted >= 0) sockets::close(wasted);
            idleFd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
            WT_LOG_WARN << "EMFILE: dropped one incoming connection via idleFd";
            break;
        }
        default:
            WT_LOG_ERROR << "accept failed errno=" << savedErrno;
            break;
    }
}

} // namespace nwl
