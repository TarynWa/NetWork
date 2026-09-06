#ifndef NWL_INETADDRESS_HPP
#define NWL_INETADDRESS_HPP
// InetAddress：sockaddr_in/6 封装（对齐 chatserver 现有 toIpPort() 用法）
#include <netinet/in.h>
#include <string>

namespace nwl {

class InetAddress {
public:
    /// 监听构造：绑定 [ip:]port，family 默认 IPv4
    explicit InetAddress(uint16_t port = 0,
                         const std::string& ip = std::string(),
                         sa_family_t family = AF_INET);

    /// 由 getsockname/getpeername 结果构造
    explicit InetAddress(const struct sockaddr_in6& addr6) : addr6_(addr6) {}

    const struct sockaddr* getSockAddr() const { return reinterpret_cast<const struct sockaddr*>(&addr6_); }
    void setSockAddrInet6(const struct sockaddr_in6& a) { addr6_ = a; }
    sa_family_t family() const { return addr6_.sin6_family; }

    uint16_t port() const;
    std::string ip() const;
    std::string toIpPort() const;

private:
    // 统一以 v6 结构承载，v4 前缀映射布局一致
    struct sockaddr_in6 addr6_{};
};

} // namespace nwl

#endif // NWL_INETADDRESS_HPP
