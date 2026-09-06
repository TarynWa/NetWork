#ifndef NWL_SOCKET_HPP
#define NWL_SOCKET_HPP
// Socket：fd 的 RAII 封装（析构自动 close，杜绝泄漏）
#include <netinet/in.h>
#include "nwl/InetAddress.hpp"

// 仅供内联构造器使用的前置声明；完整定义见 src/util/SocketsOps.hpp
namespace nwl::sockets {
int createNonblockingOrDie(sa_family_t family);
}

namespace nwl {

class Socket {
public:
    explicit Socket(sa_family_t family) { sockfd_ = sockets::createNonblockingOrDie(family); }
    /// 接管已存在的 fd（TcpConnection/Acceptor 场景）
    explicit Socket(int existingFd) : sockfd_(existingFd) {}
    ~Socket();

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    int fd() const { return sockfd_; }
    bool valid() const { return sockfd_ >= 0; }

    void setTcpNoDelay(bool on);
    void setReuseAddr(bool on);
    void setReusePort(bool on);

    void bindAddress(const InetAddress& addr);
    void listen();
    /// 接受连接；对端地址写入 peer，失败返回 -1
    int accept(InetAddress* peer);
    void shutdownWrite();

    InetAddress getLocalAddr() const;
    InetAddress getPeerAddr() const;

private:
    int sockfd_;
};

} // namespace nwl

#endif // NWL_SOCKET_HPP
