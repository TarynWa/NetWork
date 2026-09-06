#include "nwl/InetAddress.hpp"
#include <netinet/in.h>
#include <cstring>
#include "../util/SocketsOps.hpp"

namespace nwl {

InetAddress::InetAddress(uint16_t port, const std::string& ip, sa_family_t family) {
    ::memset(&addr6_, 0, sizeof addr6_);
    addr6_.sin6_family = static_cast<sa_family_t>(family);
    // 统一通过 v4/v6 布局前缀一致性写入端口（网络字节序）
    reinterpret_cast<struct sockaddr_in*>(&addr6_)->sin_port = ::htons(port);
    if (family == AF_INET) {
        auto* sin = reinterpret_cast<struct sockaddr_in*>(&addr6_);
        sin->sin_addr.s_addr = ip.empty()
            ? INADDR_ANY
            : ::inet_addr(ip.c_str());
    } else if (family == AF_INET6 && !ip.empty()) {
        ::inet_pton(AF_INET6, ip.c_str(), &addr6_.sin6_addr);
    }
}

uint16_t InetAddress::port() const { return sockets::toPort(addr6_); }

std::string InetAddress::ip() const { return sockets::toIp(addr6_); }

std::string InetAddress::toIpPort() const { return sockets::toIpPort(addr6_); }

} // namespace nwl
