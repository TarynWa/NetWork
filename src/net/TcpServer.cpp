#include "nwl/TcpServer.hpp"
#include <cassert>
#include <cstdio>
#include <Logger.hpp>
#include "../util/SocketsOps.hpp"
#include "Acceptor.hpp"
#include "EventLoopThreadPool.hpp"
#include "nwl/EventLoop.hpp"
#include "nwl/TcpConnection.hpp"

namespace nwl {

TcpServer::TcpServer(EventLoop* loop, const InetAddress& listenAddr,
                     std::string name, Option option)
    : baseLoop_(loop),
      name_(std::move(name)),
      acceptor_(new Acceptor(loop, listenAddr, option == Option::kReusePort)),
      pool_(new EventLoopThreadPool(loop, name_ + "-pool")) {
    acceptor_->setNewConnectionCallback(
        [this](int sockfd, const InetAddress& peer) { handleNewConnection(sockfd, peer); });
    WT_LOG_INFO << "TcpServer [" << name_ << "] created at "
                << listenAddr.toIpPort();
}

TcpServer::~TcpServer() {
    baseLoop_->assertInLoopThread();
    WT_LOG_INFO << "TcpServer [" << name_ << "] destructing";
    std::lock_guard<std::mutex> lock(connMutex_);
    for (auto& item : connections_) {
        TcpConnPtr conn(item.second);
        item.second.reset();
        conn->getLoop()->runInLoop([conn] { conn->forceCloseInLoop(); });
    }
}

void TcpServer::setThreadNum(int numThreads) {
    pool_->setThreadNum(numThreads);
}

void TcpServer::start() {
    if (!started_) {
        started_ = true;
        pool_->start();                 // 先起 IO 线程，再开监听防漏连接
        baseLoop_->runInLoop([this] { acceptor_->listen(); });   // 幂等
    }
    WT_LOG_INFO << "TcpServer [" << name_ << "] started with "
                << pool_->threadNum() << " io thread(s)";
}

void TcpServer::handleNewConnection(int sockfd, const InetAddress& peer) {
    baseLoop_->assertInLoopThread();

    // round-robin 挑 IO loop（未配置线程时即 baseLoop）
    EventLoop* ioLoop = pool_->getNextLoop();
    char buf[64];
    std::snprintf(buf, sizeof buf, "#%d", nextConnId_.fetch_add(1));
    std::string connName = name_ + buf;
    const InetAddress local(sockets::getLocalAddr(sockfd));

    auto conn = std::make_shared<TcpConnection>(
        ioLoop, connName, sockfd, local, peer);

    // 连接表跨环并发访问：handleNewConnection 在 baseLoop，removeConnection 在
    // 各连接所属 IO 线程（muduo 同款设计，须加锁）
    {
        std::lock_guard<std::mutex> lock(connMutex_);
        connections_[connName] = conn;
    }

    conn->setConnectionCallback(connectionCallback_);
    conn->setMessageCallback(messageCallback_);
    conn->setWriteCompleteCallback(writeCompleteCallback_);
    conn->setCloseCallback(
        [this](const TcpConnPtr& c) { removeConnection(c); });

    ioLoop->runInLoop([conn] { conn->connectEstablished(); });
    WT_LOG_INFO << "new connection [" << connName << "] from "
                << peer.toIpPort() << " -> ioLoop "
                << static_cast<const void*>(ioLoop);
}

void TcpServer::removeConnection(const TcpConnPtr& conn) {
    // 由 TcpConnection::handleClose 在连接所属 IO 线程同步调用，
    // 因此这里可能运行在任意 IO loop —— 严禁断言 baseLoop（曾在此 abort）
    {
        std::lock_guard<std::mutex> lock(connMutex_);
        connections_.erase(conn->name());
    }

    // 跨环提升代：把 shared_ptr 一路带到 connectDestroyed 栈帧结束，
    // 期间宿主对象绝对存活，杜绝析构竞态窗口
    EventLoop* ioLoop = conn->getLoop();
    ioLoop->queueInLoop([conn] { conn->connectDestroyed(); });
}

} // namespace nwl
