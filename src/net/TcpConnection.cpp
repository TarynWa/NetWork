#include "nwl/TcpConnection.hpp"
#include <sys/socket.h>
#include <cerrno>
#include <Logger.hpp>
#include "nwl/EventLoop.hpp"

namespace nwl {

TcpConnection::TcpConnection(EventLoop* loop,
                             std::string name,
                             int sockfd,
                             const InetAddress& localAddr,
                             const InetAddress& peerAddr)
    : loop_(loop),
      name_(std::move(name)),
      socket_(new Socket(sockfd)),            // 接管 Acceptor 交付的 fd
      channel_(new Channel(loop, sockfd)),
      localAddr_(localAddr),
      peerAddr_(peerAddr) {
    // 此处只能捕获 this：构造函数内 shared_from_this() 未就绪（宿主由 TcpServer
    // 以 make_shared 创建）。安全性由 connectEstablished 中 channel_->tie(self)
    // 的弱引用守卫兜底——事件分发前宿主必然已建立 tie。
    channel_->setReadCallback([this](Timestamp t) { handleRead(t); });
    channel_->setWriteCallback([this] { handleWrite(); });
    channel_->setCloseCallback([this] { handleClose(); });
    channel_->setErrorCallback([this] { handleError(); });
}

void TcpConnection::connectEstablished() {
    loop_->assertInLoopThread();
    uint8_t expect = kConnecting;
    if (!state_.compare_exchange_strong(expect, kConnected)) {
        return;
    }
    channel_->tie(getSelf());         // 宿主弱引用：in-flight 事件 UAF 第一防线
    channel_->enableReading();        // 关注可读

    if (connectionCallback_) connectionCallback_(getSelf());
}

TcpConnection::~TcpConnection() {
    // fd 归宿：由成员 socket_（RAII）在对象销毁时关闭；
    // 事件表注销已在 connectDestroyed 于所属 loop 内先行完成
}

void TcpConnection::send(const std::string& message) {
    if (state_.load() != kConnected) return;
    if (loop_->isInLoopThread()) {
        sendInLoop(message);
        return;
    }
    // 跨线程：值拷贝进闭包保证生命周期，回投所属 loop
    loop_->queueInLoop([self = getSelf(), message] { self->sendInLoop(message); });
}

void TcpConnection::send(const char* data, size_t len) {
    send(std::string(data, len));
}

void TcpConnection::sendInLoop(const std::string& message) {
    loop_->assertInLoopThread();
    if (state_.load() != kConnected) return;
    const bool hasPending = outputBuffer_.readableBytes() > 0 || channel_->isWriting();

    if (!hasPending) {                // 首发：直写内核，避免无谓拷贝
        ssize_t nwrote = ::send(channel_->fd(), message.data(),
                                message.size(), MSG_NOSIGNAL);
        if (nwrote >= 0) {
            if (static_cast<size_t>(nwrote) < message.size()) {
                // 部分写成功 → 余量入队等 EPOLLOUT 续传
                const char* restBegin = message.data() + nwrote;
                size_t restLen = message.size() - static_cast<size_t>(nwrote);
                outputBuffer_.append(restBegin, restLen);
                channel_->enableWriting();
            } else if (writeCompleteCallback_) {   // 一次写净且用户关心完成时机
                auto self = getSelf();
                loop_->queueInLoop([self] { self->writeCompleteCallback_(self); });
            }
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            handleError();
        }
        return;
    }

    const size_t before = outputBuffer_.readableBytes();
    outputBuffer_.append(message.data(), message.size());
    if (before < highWaterMark_ &&
        outputBuffer_.readableBytes() >= highWaterMark_ && highWaterMarkCallback_) {
        highWaterMarkCallback_(getSelf(), outputBuffer_.readableBytes());
    }
}

void TcpConnection::shutdown() {
    uint8_t cur = kConnected;
    if (!state_.compare_exchange_strong(cur, kDisconnecting)) return;

    // 本线程即所属 loop 直接半关；否则回投后执行
    if (loop_->isInLoopThread()) shutdownInLoop();
    else loop_->queueInLoop([self = getSelf()] { self->shutdownInLoop(); });
}

void TcpConnection::shutdownInLoop() {
    loop_->assertInLoopThread();
    if (!channel_->isWriting()) {
        socket_->shutdownWrite();     // SHUT_WR；对端随后 FIN 回流触发 handleClose
    }
    // 仍有积压时保持等待 handleWrite 写净后再收口
}

void TcpConnection::forceClose() {
    // 状态机容错：kConnected→kDisconnecting 转换失败（如已 Disconnected）时，
    // 下游 handleClose 的状态守卫会自动短路
    uint8_t cur = kConnected;
    state_.compare_exchange_strong(cur, kDisconnecting);
    auto self = getSelf();
    loop_->runInLoop([self] { self->forceCloseInLoop(); });   // runInLoop 保序回主路径
}

void TcpConnection::forceCloseInLoop() {
    loop_->assertInLoopThread();
    if (state_.load() == kConnected || state_.load() == kDisconnecting) {
        handleClose();
    }
}

void TcpConnection::setTcpNoDelay(bool on) {
    socket_->setTcpNoDelay(on);
}

void TcpConnection::handleRead(Timestamp receiveTime) {
    int savedErrno = 0;
    ssize_t n = inputBuffer_.readFd(channel_->fd(), &savedErrno);
    if (n > 0) {
        messageCallback_(getSelf(), &inputBuffer_, receiveTime);
    } else if (n == 0) {
        handleClose();                            // 对端 FIN
    } else {
        errno = savedErrno;
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            WT_LOG_ERROR << "readv failed on [" << name_ << "] errno=" << errno;
            handleError();
        }
    }
}

void TcpConnection::handleWrite() {
    loop_->assertInLoopThread();
    if (!channel_->isWriting()) {
        // 无关注写字节却收到 EPOLLOUT，忽略即可
        return;
    }
    if (outputBuffer_.readableBytes() > 0) {
        ssize_t nwrote = ::send(channel_->fd(), outputBuffer_.peek(),
                                outputBuffer_.readableBytes(), MSG_NOSIGNAL);
        if (nwrote > 0) {
            outputBuffer_.retrieve(static_cast<size_t>(nwrote));
            if (outputBuffer_.readableBytes() == 0) {
                channel_->disableWriting();       // 写尽即撤 EPOLLOUT 防 busy loop
                if (writeCompleteCallback_) {
                    auto self = getSelf();
                    loop_->queueInLoop([self] { self->writeCompleteCallback_(self); });
                }
                if (state_.load() == kDisconnecting) {
                    socket_->shutdownWrite();     // shutdown 排空后到点收口
                }
            }
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            WT_LOG_ERROR << "TcpConnection::handleWrite [" << name_ << "] errno=" << errno;
            handleError();
        }
    } else {
        channel_->disableWriting();               // 双保险，防漏 disable
    }
}

void TcpConnection::handleClose() {
    loop_->assertInLoopThread();
    uint8_t st = state_.load();
    if (!(st == kConnected || st == kDisconnecting)) return;

    // closeRequested_：对端 FIN、RST、用户 forceClose 殊途同归，只放行一次，
    // 防竞态下重复 closeCallback 造成连接表二次摘除 / Poller 重复注销
    if (closeRequested_) return;
    closeRequested_ = true;

    channel_->disableAll();
    reading_ = false;
    // 对齐 muduo：这里不提前落 kDisconnected，由所属环的 connectDestroyed
    // 统一收口，否则正常对端断开会丢掉一次 "conn down" 用户回调

    auto self = getSelf();                        // 提升代穿越未来跨环销毁窗口
    if (closeCallback_) closeCallback_(self);
}

void TcpConnection::handleError() {
    InetAddress peer = socket_->getPeerAddr();
    WT_LOG_ERROR << "TcpConnection::handleError [" << name_ << "] peer=" << peer.toIpPort()
                 << " SO_ERRNO=" << errno;
}

void TcpConnection::connectDestroyed() {
    loop_->assertInLoopThread();
    if (state_.load() == kConnected) {            // 服务端主动断开也走统一 onClose 通知
        state_.store(kDisconnected);
        channel_->disableAll();
        if (connectionCallback_) connectionCallback_(getSelf());
    }
    channel_->remove();                           // 从 Poller 表注销 + kNew 复位
    // 注：本对象经由 closeCallback 捕获的 shared_ptr 在 TcpServer::removeConnection
    // 栈帧结束后归零，析构中 ::close(fd)
}

} // namespace nwl
