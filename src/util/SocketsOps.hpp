#ifndef NWL_SOCKETSOPS_HPP
#define NWL_SOCKETSOPS_HPP
// SocketsOps：底层 socket C API 薄封装（内部实现头，不对外暴露）
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>

namespace nwl {
namespace sockets {

/// 创建非阻塞 + CLOEXEC 的 socket，失败直接 abort
int createNonblockingOrDie(sa_family_t family);

/// 绑定地址（端口 0 表示随机分配）
void bindOrDie(int sockfd, const struct sockaddr* addr);

/// 进入监听
void listenOrDie(int sockfd);

/// accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)；EMFILE 等错误返回 -1（errno 已置）
int accept(int sockfd, struct sockaddr_in6* addr);

void close(int fd);
void shutdownWrite(int fd);

/// 带连接的读套接字名址提取（TcpConnection 构造时用）
struct sockaddr_in6 getLocalAddr(int sockfd);
struct sockaddr_in6 getPeerAddr(int sockfd);

/// v4/v6 通用信息（v4 通过前 8 字节布局一致性读取）
std::string toIp(const struct sockaddr_in6& addr6);
std::string toIpPort(const struct sockaddr_in6& addr6);
uint16_t    toPort(const struct sockaddr_in6& addr6);

inline const struct sockaddr* sockaddrCast(const struct sockaddr_in6* p) {
    return reinterpret_cast<const struct sockaddr*>(p);
}

} // namespace sockets
} // namespace nwl

#endif // NWL_SOCKETSOPS_HPP
