#include "SocketsOps.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include "Logger.hpp"

namespace nwl {
namespace sockets {

namespace detail {
[[noreturn]] void fatal(const char* what, int savedErrno) {
    WT_LOG_FATAL << what << " errno=" << savedErrno;
    ::abort();
}
} // namespace detail

int createNonblockingOrDie(sa_family_t family) {
    int fd = ::socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (fd < 0) {
        detail::fatal("sockets::createNonblockingOrDie", errno);
    }
    return fd;
}

void bindOrDie(int sockfd, const struct sockaddr* addr) {
    if (::bind(sockfd, addr, static_cast<socklen_t>(sizeof(struct sockaddr_in6))) < 0) {
        detail::fatal("sockets::bindOrDie", errno);
    }
}

void listenOrDie(int sockfd) {
    if (::listen(sockfd, SOMAXCONN) < 0) {
        detail::fatal("sockets::listenOrDie", errno);
    }
}

int accept(int sockfd, struct sockaddr_in6* addr) {
    socklen_t addrlen = static_cast<socklen_t>(sizeof *addr);
    // accept4 一步到位拿到非阻塞 + CLOEXEC（plan.md §3.8）
    int connfd = ::accept4(sockfd,
                           reinterpret_cast<struct sockaddr*>(addr),
                           &addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
    return connfd;   // EMFILE 由 Acceptor 的 idleFd 兜底处理
}

void close(int fd) {
    if (::close(fd) < 0) {
        WT_LOG_ERROR << "sockets::close fd=" << fd << " errno=" << errno;
    }
}

void shutdownWrite(int fd) {
    if (::shutdown(fd, SHUT_WR) < 0 && errno != ENOTCONN) {
        WT_LOG_ERROR << "sockets::shutdownWrite fd=" << fd << " errno=" << errno;
    }
}

struct sockaddr_in6 getLocalAddr(int sockfd) {
    struct sockaddr_in6 addr6{};
    socklen_t len = sizeof addr6;
    if (::getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&addr6), &len) < 0) {
        detail::fatal("sockets::getLocalAddr", errno);
    }
    return addr6;
}

struct sockaddr_in6 getPeerAddr(int sockfd) {
    struct sockaddr_in6 addr6{};
    socklen_t len = sizeof addr6;
    if (::getpeername(sockfd, reinterpret_cast<struct sockaddr*>(&addr6), &len) < 0) {
        detail::fatal("sockets::getPeerAddr", errno);
    }
    return addr6;
}

// v4 与 v6 的 sin_family/sin_port 字段布局一致，muduo 同款技巧：统一按 v6 读取
std::string toIp(const struct sockaddr_in6& addr6) {
    char buf[INET6_ADDRSTRLEN] = "";
    ::inet_ntop(addr6.sin6_family,
                addr6.sin6_family == AF_INET
                    ? static_cast<const void*>(&reinterpret_cast<const struct sockaddr_in*>(&addr6)->sin_addr)
                    : static_cast<const void*>(&addr6.sin6_addr),
                buf, sizeof buf);
    return buf;
}

uint16_t toPort(const struct sockaddr_in6& addr6) {
    return ::ntohs(reinterpret_cast<const struct sockaddr_in*>(&addr6)->sin_port);
}

std::string toIpPort(const struct sockaddr_in6& addr6) {
    return toIp(addr6) + ":" + std::to_string(toPort(addr6));
}

} // namespace sockets
} // namespace nwl
