#ifndef NWL_TCP_CONNECTION_HPP
#define NWL_TCP_CONNECTION_HPP
// TcpConnection：连接封装（plan.md §3.7 状态机）
// 生命周期三重保险：enable_shared_from_this + Channel::tie + closeCallback 跨环提升代
#include <atomic>
#include <memory>
#include <string>
#include "nwl/Buffer.hpp"
#include "nwl/Channel.hpp"
#include "nwl/Callbacks.hpp"
#include "nwl/InetAddress.hpp"
#include "nwl/Socket.hpp"

namespace nwl {

class EventLoop;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    TcpConnection(EventLoop* loop,
                  std::string name,
                  int sockfd,
                  const InetAddress& localAddr,
                  const InetAddress& peerAddr);
    ~TcpConnection();

    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;

    // ---- 对外查询 ----
    EventLoop* getLoop() const { return loop_; }
    const std::string& name() const { return name_; }
    const InetAddress& localAddress() const { return localAddr_; }
    const InetAddress& peerAddress() const { return peerAddr_; }
    bool connected() const { return state_.load() == kConnected; }
    bool disconnected() const { return state_.load() == kDisconnected; }

    /// 返回可用于延长生命周期的自持指针
    std::shared_ptr<TcpConnection> getSelf() { return shared_from_this(); }

    // ---- 发送与关闭（任意线程可调用）----
    void send(const std::string& message);
    void send(const char* data, size_t len);
    void shutdown();          // 优雅半关闭：先排空 outputBuffer
    void forceClose();        // 立即关闭

    /// 仅供 TcpServer 析构清理路径调用（跳过 runInLoop 二次转投）
    void forceCloseInLoop();

    void setTcpNoDelay(bool on);

    // ---- 回调注册（由 TcpServer 注入）----
    void setConnectionCallback(const ConnectionCallback& cb)     { connectionCallback_ = cb; }
    void setMessageCallback(const MessageCallback& cb)           { messageCallback_ = cb; }
    void setWriteCompleteCallback(const WriteCompleteCallback& cb) { writeCompleteCallback_ = cb; }
    void setHighWaterMarkCallback(const HighWaterMarkCallback& cb, size_t mark) {
        highWaterMarkCallback_ = cb;
        highWaterMark_ = mark;
    }
    void setCloseCallback(const CloseCallback& cb) { closeCallback_ = cb; }

    // 由 TcpServer 在所属 loop 内调用（连接生命周期两端）
    void connectEstablished();
    void connectDestroyed();

private:
    enum StateE : uint8_t { kDisconnected, kConnecting, kConnected, kDisconnecting };

    void handleRead(Timestamp receiveTime);
    void handleWrite();
    void handleClose();
    void handleError();

    void sendInLoop(const std::string& message);
    void shutdownInLoop();

    EventLoop* loop_;
    const std::string name_;
    std::atomic<uint8_t> state_{kConnecting};   // 主读主写在 IO 线程；send/shutdown 跨线程 ⇒ 原子态
    bool reading_ = true;
    bool closeRequested_ = false;               // 仅所属 loop 线程读写：closeCallback 幂等闸门

    std::unique_ptr<Socket> socket_;
    std::unique_ptr<Channel> channel_;

    InetAddress localAddr_;
    InetAddress peerAddr_;

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;
    HighWaterMarkCallback highWaterMarkCallback_;
    CloseCallback closeCallback_;
    static constexpr size_t kHighWaterMark = 64 * 1024 * 1024;   // 64MB 背压告警线
    size_t highWaterMark_ = kHighWaterMark;

    Buffer inputBuffer_;
    Buffer outputBuffer_;   // 未写完的数据排队区（EPOLLOUT 驱动续写）
};

} // namespace nwl

#endif // NWL_TCP_CONNECTION_HPP
