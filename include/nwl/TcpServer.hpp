#ifndef NWL_TCP_SERVER_HPP
#define NWL_TCP_SERVER_HPP
// TcpServer：组装 Acceptor + EventLoopThreadPool + 连接表（plan.md §5.5）
// 回调签名与 muduo/chatsystem 同构，chatserver.hpp 平替即迁移
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include "nwl/Buffer.hpp"          // MessageCallback 参数需完整类型
#include "nwl/Callbacks.hpp"
#include "nwl/InetAddress.hpp"
#include "nwl/TcpConnection.hpp"

namespace nwl {

class Acceptor;
class EventLoop;
class EventLoopThreadPool;

using TcpConnPtr = std::shared_ptr<TcpConnection>;

class TcpServer {
public:
    enum class Option { kNoReusePort, kReusePort };

    TcpServer(EventLoop* loop,
              const InetAddress& listenAddr,
              std::string name,
              Option option = Option::kNoReusePort);
    ~TcpServer();

    void setThreadNum(int numThreads);      // 定义在 cpp（pool_ 为不完整类型）

    void setConnectionCallback(ConnectionCallback cb)      { connectionCallback_ = std::move(cb); }
    void setMessageCallback(MessageCallback cb)            { messageCallback_ = std::move(cb); }
    void setWriteCompleteCallback(WriteCompleteCallback cb){ writeCompleteCallback_ = std::move(cb); }

    /// 启动监听与 IO 线程；可多次调用（幂等）
    void start();

private:
    /// Acceptor 命中新连接：挑一个 IO loop 构造 TcpConnection
    void handleNewConnection(int sockfd, const InetAddress& peer);

    /// closeCallback 落点：在连接所属 IO 线程执行（TcpConnection::handleClose），
    /// 多环并发 → 摘表须持锁；随后回所属环销毁事件句柄
    void removeConnection(const TcpConnPtr& conn);

    EventLoop* baseLoop_;                  // 仅 accept + 分发
    const std::string name_;
    std::unique_ptr<Acceptor> acceptor_;
    std::shared_ptr<EventLoopThreadPool> pool_;

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;

    using ConnectionMap = std::unordered_map<std::string, TcpConnPtr>;
    mutable std::mutex connMutex_;         // connections_ 会被多个 IO 环并发增删
    ConnectionMap connections_;
    std::atomic<int> nextConnId_{1};
    bool started_ = false;                 // 主线程独占访问
};

} // namespace nwl

#endif // NWL_TCP_SERVER_HPP
