#include "nwl/Socket.hpp"
#include <sys/socket.h>
#include "../util/SocketsOps.hpp"
namespace nwl {

Socket::~Socket() {
    if (sockfd_ >= 0) {
        sockets::close(sockfd_);
    }
}

void Socket::setTcpNoDelay(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(sockfd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof opt);
}

void Socket::setReuseAddr(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);
}

void Socket::setReusePort(bool on) {
#ifdef SO_REUSEPORT
    int opt = on ? 1 : 0;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof opt);
#else
    (void)on;
#endif
}

void Socket::bindAddress(const InetAddress& addr) {
    sockets::bindOrDie(sockfd_, addr.getSockAddr());
}

void Socket::listen() {
    sockets::listenOrDie(sockfd_);
}

int Socket::accept(InetAddress* peer) {
    struct sockaddr_in6 addr6{};
    int connfd = sockets::accept(sockfd_, &addr6);
    if (connfd >= 0 && peer) {
        peer->setSockAddrInet6(addr6);
    }
    return connfd;
}

void Socket::shutdownWrite() {
    sockets::shutdownWrite(sockfd_);
}

InetAddress Socket::getLocalAddr() const {
    return InetAddress(sockets::getLocalAddr(sockfd_));
}

InetAddress Socket::getPeerAddr() const {
    return InetAddress(sockets::getPeerAddr(sockfd_));
}

} // namespace nwl
