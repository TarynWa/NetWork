# NetWorkLibrary · 方案一落地计划：muduo 风格 One-Loop-Per-Thread Reactor（纯 epoll 实现）

> 基于 [task.md](task.md) §2.1 选型结论，输出**可执行**的项目架构、模块设计、代码骨架与分阶段交付计划。
> 目标目录：`src/NetWorkLibrary/`，构建产物 `libnwl.a`；完成后 `chatsystem/chatserver` 由 muduo 无缝切换到本库。

---

## 一、环境基线与依赖结论（本机实测）

| 项目 | 实测值 | 结论 |
|------|--------|------|
| 内核 | `6.6.87.2-microsoft-standard-WSL2` | ✅ epoll 全量可用，无需特性探测降级 |
| 编译器 | GCC 15.2.0 | ✅ C++17（与根 [CMakeLists.txt](CMakeLists.txt) 一致） |
| 第三方依赖 | **零** | 纯 POSIX/Linux syscall 实现；不需要安装任何包（对比方案三需 liburing 的核心优势） |
| 可复用资产 | `WT_LOG_*` 日志桥接、`SyncQueue/ScheduleThreadPool` 业务线程池、4 字节大端分帧协议 | 回调签名与 muduo 同构，chatsystem 迁移面收敛到 `chatserver.{hpp,cpp}` 两个文件 |

---

## 二、总体架构

```
┌───────────────────────────────────────────────────────────────────┐
│  应用层   chatservice / echo demo                                  │
│           onConnection(TcpConnectionPtr)                          │
│           onMessage(TcpConnectionPtr, Buffer*, Timestamp)          │
├───────────────────────────────────────────────────────────────────┤
│  会话层   TcpServer ── Acceptor(listenfd+Channel)                  │
│                 │  round-robin                                    │
│  ┌────────────▼────────────┬──────────────────┐                    │
│  │ SubReactor #0           │ SubReactor #N    │  EventLoopThreadPool│
│  │ EventLoop               │ EventLoop        │  (IO 线程, one-loop-│
│  │ ├ EpollPoller (LT)      │ ├ ...            │   per-thread)      │
│  │ ├ TimerQueue(timerfd)   │                  │                    │
│  │ └ wakeup eventfd        │                  │                    │
│  └────┬────────────────────┴──────┬───────────┘                    │
│       │ TcpConnection(inputBuf/outputBuf)                           │
├───────▼───────────────────────────────────────────────────────────┤
│  业务卸载层（复用现有 src/threadpool）                              │
│  ScheduleThreadPool::Submit(DB查询/Proto解析)                      │
│       → 结果回投 loop->queueInLoop([conn]{ conn->send(...) })      │
├───────────────────────────────────────────────────────────────────┤
│  OS 层：epoll_wait(LT) / timerfd / eventfd / accept4                │
└───────────────────────────────────────────────────────────────────┘
```

### 2.1 三类线程职责与通信

| 线程类别 | 数量 | 职责 | 进入方式 |
|----------|------|------|----------|
| main Loop | 1 | 仅 accept + 连接分发 | 主线程执行 |
| IO Loop（SubReactor） | N（默认 = min(硬件核数,4)，chatserver 用 4） | epoll 派发、Buffer 收发、Timer 触发 | `EventLoopThread` 启动并阻塞于 `loop()` |
| Business ThreadPool | M（复用项目现有线程池） | DB/Redis/重业务 | IO 回调内 `Submit()` 提交 |

**跨线程铁律**：任何非本 loop 线程修改 Channel/TcpConnection 状态，必须经 `EventLoop::runInLoop / queueInLoop` 转移回所属 IO 线程——与现有 chatservice 用 `runInLoop` 回写连接的习惯完全一致。

---

## 三、模块详细设计

### 3.1 类总览（预估 ~3500 行核心代码）

| 类 | 行数 | 职责 | 关键成员 |
|----|------|------|----------|
| `Channel` | ~120 | fd 事件绑定器 | `fd_ events_ revents_ tie_ handlers_ index_` |
| `Poller`（纯虚） | ~60 | 多路复用抽象 | `update(Channel*) removeChannel(Channel*) poll(timeout)` |
| `EpollPoller : Poller` | ~180 | epoll 封装 | `epollfd_ events_[] channels_:map<fd,Channel*>` |
| `EventLoop` | ~220 | 事件循环核心 | `poller_ pendingFunctors_ wakeupFd_ timers_` |
| `WakeupEvent` | ~60 | eventfd 跨线程唤醒 | `notify() handleRead()` |
| `TimerQueue` | ~200 | 定时器管理 | `timerfd_ set<pair<Timestamp,Timer*>>` |
| `Socket` / `SocketsOps` | ~150 | fd RAII + bind/listen/accept 封装 | `SO_REUSEADDR TCP_NODELAY SIGPIPE 屏蔽` |
| `InetAddress` | ~80 | sockaddr_in 封装 | `toIpPort()`（对齐 chatserver 现有调用） |
| `Acceptor` | ~120 | 监听 + 接受 | `accept4` 非阻塞 + EMFILE idleFd 技巧 |
| `Buffer` | ~180 | 双端缓冲区 | `readerIndex_/writerIndex_ readv 读增强` |
| `TcpConnection` | ~350 | 连接封装状态机 | 双 Buffer、`enable_shared_from_this` |
| `EventLoopThread` | ~90 | loop 与 thread 绑定 | mutex+condvar 保证启动完成才返回 |
| `EventLoopThreadPool` | ~110 | N 个 IO loop 池 | `getNextLoop()` round-robin |
| `TcpServer` | ~150 | 服务器组装入口 | 回调注册、`setThreadNum`、start |
| `Timestamp` | 复用 logSystem 同款或薄封装 | 时间戳 | — |

### 3.2 EventLoop 核心机制

```cpp
class EventLoop {
public:
    void loop();                                   // 循环: poll → 分发 → doPendingFunctors
    void runInLoop(Functor f);                     // 本线程? 直接执行 : queueInLoop
    void queueInLoop(Functor f);                   // 入队 + 唤醒（即使在 loop 线程也唤醒，
                                                   //  因为此刻可能正阻塞在 pendingFunctors 执行）
    bool isInLoopThread() const;
    void assertInLoopThread();                     // 开发期防误用断言
private:
    void abortNotInLoopThread();
    std::atomic<bool> quit_{false}, looping_{false}, callingPendingFunctors_{false};
    const std::thread::id tid_;
    std::unique_ptr<Poller> poller_;
    std::vector<Functor> pendingFunctors_;         // 备份后 swap 清空再执行（无死锁关键）
    mutable std::mutex functorMutex_;
    int wakeupFd_;                                 // eventfd(EFD_NONBLOCK|EFD_CLOEXEC)
};
```

要点：
1. `doPendingFunctors` 先在锁内 `swap` 出队列，锁外执行——回调里再调 `queueInLoop` 不死锁；
2. `queueInLoop` 判断「非本线程 **或** 正在执行 pending」都唤醒，补上第二条件否则丢唤醒；
3. 每个 `EventLoop` 构造时创建自己的 `EpollPoller/wakeupFd_`，析构顺序先关 wakeup。

### 3.3 Poller 抽象与 EpollPoller（默认 LT）

```cpp
// LT 为默认：数据未读完 epoll_wait 立即再次返回，不存在 EAGAIN 漏读风险，
// 使用门槛低，与 muduo 一致。ET 作为编译期开关留待 M4 实验分支。
constexpr int kInitEventListSize = 16;   // events_ 数组按需指数扩容
constexpr int kMaxEventListSize = 1024;

int EpollPoller::poll(int timeoutMs, ChannelList* activeChannels) {
    int numEvents = ::epoll_wait(epollfd_, &*events_.begin(),
                                 static_cast<int>(events_.size()), timeoutMs);
    if (numEvents > 0) {
        fillActiveChannels(numEvents, activeChannels);
        if (numEvents == events_.size() && events_.size() < kMaxEventListSize)
            events_.resize(events_.size() * 2);         // 动态扩容
    } else if (numEvents < 0 && errno != EINTR) {
        WT_LOG_ERROR << "EPollPoller::poll() err";
    }
    return numEvents;
}

void EpollPoller::updateChannel(Channel* ch) {          // EPOLL_CTL_ADD/MOD 按 index_ 区分
    struct epoll_event ev{}; ev.events = ch->events(); ev.data.ptr = ch;
    int op = ch->isNoneEvent() ? EPOLL_CTL_DEL : (ch->index_ < 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD);
    ::epoll_ctl(epollfd_, op, ch->fd(), &ev);
}
```

`data.ptr` 直接挂 `Channel*`（不存 fd→查 map），O(1) 还原事件归属；`removeChannel` 时同步维护 map 并收缩 `events_`。

### 3.4 Channel 生命周期安全 —— tie 弱引用（UAF 第一防线）

```cpp
void Channel::tie(const std::shared_ptr<void>& owner) { // owner=TcpConnectionPtr
    tie_ = owner;
}
void Channel::handleEvent(Timestamp receiveTime) {
    std::shared_ptr<void> guard;
    if (tie_) guard = tie_.lock();
    if (!tie_ || guard)        // owner 已析构 ⇒ 本轮事件直接跳过
        handleEventWithGuard(receiveTime);
}
```

配合规则：**TcpConnection 构造后立即 `channel_->tie(shared_from_this())` + `channel_->enableReading()`**（必须在所属 loop 线程）。

### 3.5 TimerQueue 设计

```cpp
// 结构：timerfd_register on loop + 有序容器
std::set<std::pair<Timestamp, Timer*>> timers_;     // 唯一化重复时刻
addTimer(cb, when, interval):
    Timer* t = new Timer(...);
    loop_->runInLoop([this,t]{ addTimerInLoop(t); });   // 跨线程统一转主调
    return TimerId(t, t->sequence());
resetTimerfd():  计算 now 与最早到期差值
    itimerspec sp; sp.it_value = howMuchTimeFromNow(expiration_);
    ::timerfd_settime(timerfd_, 0, &sp, nullptr);
handleRead(): 取出到期 Timers 执行；周期任务 re-insert；一次 resetTimerfd
cancel():    泛化 pair{expiration, sequence} 打入 cancelingTimers_
             （sequence 保证同刻不同任务可区分）
析构安全：~TimerQueue 由 loop 线程执行；禁用外部持有裸 Timer*
```

### 3.6 Buffer 设计（API 对齐 chatserver.cpp 现有用法的硬约束）

chatserver.cpp 分帧用到了：`readableBytes() / peek() / retrieve(kHeaderLen) / retrieveAsString(len)` — 本 Buffer **必须原样提供同名方法**：

```cpp
class Buffer {
public:
    static constexpr size_t kCheapPrepend = 8;
    static constexpr size_t kInitialSize  = 1024;

    size_t readableBytes() const;         // writerIndex_ - readerIndex_
    size_t writableBytes() const;
    const char* peek() const;             // data_ + readerIndex_
    void retrieve(size_t len);            // readerIndex_ += len
    std::string retrieveAsString(size_t len);
    void append(const char* data, size_t len);

    ssize_t readFd(int fd, int* savedErrno);  // 见下：readv 一次性读尽减少 syscall

private:
    std::vector<char> buffer_;
    size_t readerIndex_ = kCheapPrepend, writerIndex_ = kCheapPrepend;
};

ssize_t Buffer::readFd(int fd, int* savedErrno) {
    char extrabuf[65536];                       // 栈上备用块，避免内核数据触发多次扩容拷贝
    struct iovec vec[2]{
        {beginWrite(), static_cast<size_t>(writableBytes())},
        {extrabuf, sizeof extrabuf},
    };
    ssize_t n = ::readv(fd, vec, 2);
    if (n <= static_cast<ssize_t>(writableBytes())) hasWritten(n);
    else { hasWritten(writableBytes()); append(extrabuf, n - writableBytes()); }
    return n;
}
```

内存布局 `[prepend 8B][readable...][writable...]`：prepend 区为后续长度前缀回填预留（协议演进友好）。

### 3.7 TcpConnection 状态机

```
                     TcpServer::newConnection(loop,idx)
                     make_shared<TcpConnection>(...)   ← 由 Acceptor 接收新 fd 后进入
                              │ ioCallback: connectEstablished()
                              ▼
        ┌──────────────┤ channel_->tie(this); enableReading()
        ▼              │ state=Connected; connectionCb_(this)
   ◄────┘              ▼
handleRead(): inputBuffer_.readFd()
   │ messageCallback_(getSelf(), &inputBuffer_, recvTime)   ← chatserver.cpp while 分帧
   │ peerClosed(n==0)/err? handleClose()
   ▼
send(string buf):    loop 所有线程可达
   ├ 本 loop 线程 → sendInLoop
   │ else → queueInLoop(std::bind(sendInLoop))
   sendInLoop():
   ├ outputBuffer 空 且 writing==false → 先试 ::write(fd,...)
   │     ├ 全部写完 → writeCompleteCb_(可选)
   │     └ EAGAIN/部分 → 追加 outputBuffer; enableWriting();(等 EPOLLOUT 再续写)
   │                        highWaterMarkCb_ 触发背压告警
   shutdown(): user 发起半关闭
   ├ Connected → loop->runInLoop(shutdownInLoop)
   │ shutdownInLoop: outputBuffer 无积压 → 关闭写端(SHUT_WR)；有积压则标记
   │                  state=Disconnecting 等 EPOLLOUT 写完自动 shutdownWrite
handleWrite(): 续写 outputBuffer → 写完 disableWriting；若 Disconnecting 则 shutdownWrite
handleClose(): state=Disconnected; disableAll; tcpNoDelay 清理; closeCallback_(guardThis)
               ↑closeCallback 由 TcpServer 注册 → removeConnection:
                 loop->queueInLoop(bind(connectDestroyed, guard))  「提升代」跨环清理
connectDestroyed(): channel_->remove(); connectionCb_(Disconnected); connectionMap_.erase(name_)
```

三条生命线（缺一即 UAF）：① `enable_shared_from_this` 让用户侧 conn 回调延长对象；② `tie()` 防 in-flight 事件悬垂；③ `closeCallback → removeConnection` 全程以 shared_ptr「提升代」跨线程移交，杜绝析构竞态窗口。

### 3.8 Acceptor 与 EMFILE 处理

```cpp
void Acceptor::handleRead() {
    InetAddress peer;
    int connfd = acceptSocket_.accept(&peer);          // ::accept4(SOCK_NONBLOCK|CLOEXEC)
    if (connfd >= 0) { newConnectionCb_(connfd, peer); }
    else if (errno == EMFILE) {
        idleFd_ 兜底:  ::close(idleFd_); idom = ::accept(listenfd_,...); ::close(idom);
                       idleFd_ = ::open("/dev/null", O_RDONLY|O_CLOEXEC);
        // 「借 fd 席位吃掉并发起的连接」，避免 fd 表满时 listen 队列风暴循环空转
    } else /* fatal */ WT_LOG_ERROR << "accept failed";
}
idleFd_(::open("/dev/null", O_RDONLY|O_CLOEXEC))   // 构造时预留
```

### 3.9 EventLoopThreadPool / EventLoopThread

```cpp
EventLoop* EventLoopThreadPool::getNextLoop() {
    EventLoop* loop = baseLoop_;
    if (!loops_.empty()) { loop = loops_[next_]; ++next_ %= loops_.size(); }
    return loop;                                       // round-robin
}
// EventLoopThread::startLoop(): 启动线程并 condvar 等其把 &loop_ 放入 lambda 后返回
// 保证 server.start() 时所有 loop 已就绪，不出现 loop 未启动就 dispatch
```

---

## 四、目录结构与 CMake

### 4.1 目录结构

```
src/NetWorkLibrary/
├── CMakeLists.txt
├── README.md
├── include/nwl/                         # 对外 API
│   ├── Callbacks.hpp                    # ConnectionCallback/MessageCallback/... typedef
│   ├── EventLoop.hpp
│   ├── Channel.hpp
│   ├── Poller.hpp
│   ├── TimerId.hpp
│   ├── TcpServer.hpp
│   ├── TcpConnection.hpp
│   ├── Buffer.hpp
│   ├── InetAddress.hpp
│   ├── Socket.hpp
│   └── Config.hpp                       # 版本宏 / NWL_WITH_ET 实验开关
├── src/
│   ├── core/
│   │   ├── EventLoop.cpp / .hpp
│   │   ├── Channel.cpp / .hpp
│   │   ├── WakeupEvent.cpp / .hpp
│   │   └── TimerQueue.cpp / .hpp        # 含 Timer/TimerId 定义
│   ├── poller/
│   │   ├── Poller.cpp / .hpp            # 工厂 newDefaultPoller()
│   │   └── EpollPoller.cpp / .hpp
│   ├── net/
│   │   ├── Acceptor.cpp / .hpp
│   │   ├── EventLoopThread.cpp / .hpp
│   │   ├── EventLoopThreadPool.cpp / .hpp
│   │   ├── TcpConnection.cpp
│   │   ├── TcpServer.cpp
│   │   ├── Socket.cpp / SocketUtil.hpp
│   │   └── InetAddress.cpp / .hpp
│   ├── util/
│   │   ├── Buffer.cpp
│   │   ├── SocketsOps.cpp / .hpp
│   │   └── Timestamp.cpp / .hpp
│   └── NetworkLibrary.cpp               # 全局初始化(SIGPIPE 忽略等)
├── examples/
│   ├── echo_server_nwl.cpp
│   └── echo_client_bench.cpp            # ping-pong 压测客户端
└── tests/
    ├── Test_Buffer.cpp                  # retrieve/readv/水位边界
    ├── Test_EventLoop.cpp               # runInLoop 跨线程次序
    ├── Test_TimerQueue.cpp              # 周期/cancel/抖动
    ├── Test_TcpConnection.cpp           # 关闭竞态/tie 生效验证
    ├── Test_Integration_Echo.cpp
    └── Test_HalfFrame.cpp               # 分帧：半包/粘包/超长包/非法长度
```

### 4.2 CMake 集成

```cmake
# src/NetWorkLibrary/CMakeLists.txt
cmake_minimum_required(VERSION 3.14)
project(nwl LANGUAGES CXX)

add_library(nwl STATIC
  src/NetworkLibrary.cpp
  src/core/EventLoop.cpp  src/core/Channel.cpp
  src/core/WakeupEvent.cpp src/core/TimerQueue.cpp
  src/poller/Poller.cpp   src/poller/EpollPoller.cpp
  src/net/Acceptor.cpp    src/net/EventLoopThread.cpp
  src/net/EventLoopThreadPool.cpp
  src/net/TcpConnection.cpp src/net/TcpServer.cpp
  src/net/Socket.cpp      src/net/InetAddress.cpp
  src/util/Buffer.cpp     src/util/SocketsOps.cpp  src/util/Timestamp.cpp
)

target_include_directories(nwl PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(nwl PUBLIC cxx_std_17)
find_package(Threads REQUIRED)
target_link_libraries(nwl PUBLIC Threads::Threads muduo_log)   # 日志桥接 WT_LOG_*
option(NWL_ENABLE_ASAN "ASan for lifetime bug hunting" OFF)
if(NWL_ENABLE_ASAN)
  target_compile_options(nwl PRIVATE -fsanitize=address -fno-omit-frame-pointer)
  target_link_options(nwl PRIVATE -fsanitize=address)
endif()
```

根 [CMakeLists.txt](CMakeLists.txt) 增加 `add_subdirectory(src/NetWorkLibrary)` 于 logSystem 之后（日志符号依赖）。

---

## 五、关键代码骨架

### 5.1 回调签名（Callbacks.hpp —— 与 muduo/chatsystem 完全同构）

```cpp
namespace nwl {
using Functor            = std::function<void()>;
using TimerCallback      = Functor;
using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
using MessageCallback    = std::function<void(const TcpConnectionPtr&, Buffer*, Timestamp)>;
using WriteCompleteCallback = std::function<void(const TcpConnectionPtr&)>;
using HighWaterMarkCallback = std::function<void(const TcpConnectionPtr&, size_t)>;
using CloseCallback      = std::function<void(const TcpConnectionPtr&)>;
} // namespace nwl
```

### 5.2 EventLoop::loop 主骨架

```cpp
void EventLoop::loop() {
    assertInLoopThread();
    looping_.store(true);
    while (!quit_.load()) {
        activeChannels_.clear();
        pollReturnTime_ = poller_->poll(kPollTimeMs, &activeChannels_);
        for (Channel* ch : activeChannels_) ch->handleEvent(pollReturnTime_);
        doPendingFunctors();                   // swap 出去锁外执行
    }
    looping_.store(false);
}

void EventLoop::quit() {
    quit_.store(true);
    if (!isInLoopThread()) wakeup();           // 跨线程退出也要唤醒
}

void EventLoop::doPendingFunctors() {
    std::vector<Functor> functors;
    callingPendingFunctors_.store(true);
    { std::lock_guard<std::mutex> lg(functorMutex_); functors.swap(pendingFunctors_); }
    for (auto& f : functors) f();              // 回调内 queueInLoop 也 OK
    callingPendingFunctors_.store(false);
}
```

### 5.3 EpollPoller 事件派发（data.ptr 直接还原 Channel）

```cpp
void EpollPoller::fillActiveChannels(int num, ChannelList* out) {
    for (int i = 0; i < num; ++i) {
        auto* ch = static_cast<Channel*>(events_[i].data.ptr);
        ch->set_revents(events_[i].events);
        out->push_back(ch);
    }
}
```

### 5.4 TcpConnection 关键路径

```cpp
void TcpConnection::handleRead(Timestamp t) {
    ssize_t n = inputBuffer_.readFd(channel_->fd(), &errno_);   // readv 一次读尽
    if (n > 0)          messageCallback_(getSelf(), &inputBuffer_, t);
    else if (n == 0)    handleClose();                          // 对端 FIN
    else                handleError();
}

void TcpConnection::sendInLoop(std::string_view msg) {
    if (state_ != Connected) return;
    if (!channel_->isWriting() && outputBuffer_.readableBytes() == 0) {
        ssize_t nwrote = ::write(fd_, msg.data(), msg.size());  // 首发直写避免 memcpy
        if (nwrote >= 0) {
            if (static_cast<size_t>(nwrote) < msg.size()) {
                outputBuffer_.append(msg.data() + nwrote, msg.size() - nwrote);
                if (!channel_->isWriting()) channel_->enableWriting(); // 等 EPOLLOUT
            } else if (writeCompleteCallback_) loop_->queueInLoop(
                    [self = getSelf()] { self->writeCompleteCallback_(self); });
        } else if (errno != EAGAIN && errno != EWOULDBLOCK)
            handleError();
    } else outputBuffer_.append(msg.data(), msg.size());        // 正在写则排队
}

void TcpServer::removeConnection(const TcpConnectionPtr& conn) {
    // 连接归哪个 sub loop 就回哪个 loop 清理（跨 loop 提升代转移）
    ioLoops_[connIdx]->queueInLoop([this, conn] { conn->connectDestroyed(); });
    connectionSet_.erase(conn);
}
```

### 5.5 TcpServer 组装示例（可直接替换 chatsystem/chatserver）

```cpp
int main() {
    nwl::GlobalInit init;                       // 忽略 SIGPIPE 等
    nwl::EventLoop loop;                        // main loop
    nwl::TcpServer server(&loop,
        nwl::InetAddress(6000), "chat_node_A");
    server.setThreadNum(4);                     // 4 个 SubReactor IO 线程
    server.setConnectionCallback([](const nwl::TcpConnPtr& c){
        WT_LOG_INFO << (c->connected() ? "connected" : "disconnected")
                    << c->peerAddress().toIpPort().c_str();
        // ↓ 与现 chatservice::clientCloseException 保持对接
    });
    server.setMessageCallback([](const nwl::TcpConnPtr& c, nwl::Buffer* b, nwl::Timestamp t){
        // —— 原 chatserver.cpp 分帧逻辑逐行平移（4 字节大端前缀 + 16MB 上限校验）——
        while (b->readableBytes() >= 4) {
            int32_t be32; ::memcpy(&be32, b->peek(), 4);
            int32_t len = ntohl(be32);
            if (len <= 0 || len > 16 * 1024 * 1024) { c->shutdown(); return; }
            if (static_cast<int32_t>(b->readableBytes()) < 4 + len) break;
            b->retrieve(4);
            std::string payload = b->retrieveAsString(len);
            // chatservice::instance()->recvmsg(c, payload, t) — 原样调用
        }
    });
    server.start();
    loop.loop();
}
```

---

## 六、易错点 Checklist（开发期逐条自检）

| # | 易错点 | 自检方法 |
|---|--------|----------|
| C1 | sendInLoop 忘判 `isWriting()` 造成重复 enableWriting → epoll_ctl MOD 冗余 | 断言同一 Channel 单轮至多一次 ADD |
| C2 | shutdown 时 outputBuffer 未排空直接关闭 → 末帧丢失 | Test_TcpConnection 覆盖「shutdown 前塞满积压」场景 |
| C3 | doPendingFunctors 锁内直接执行回调 → 死锁回环 | code review 强制「swap-out-then-run」模板 |
| C4 | 用户在 messageCallback 里销毁 TcpServer 导致遍历 activeChannels 时 UAF | 提供文档约定 + Server::start 后禁止跨线程 destroy |
| C5 | 忽略了低水位写完成场景的 writeCompleteCallback 只触发一次 | 单测覆写两段式发送 |
| C6 | removeChannel 时 EPOLL_CTL_DEL 但 fd 已被上层 close → ENOENT | Channel 记录 index_ 状态并与 Poller 双向同步 |
| C7 | WSL2 下 timerfd_settime 精度波动 | Test_TimerQueue 允许 ±5ms 容差记录基线 |
| C8 | 半包读取后 readerIndex 越过 writerIndex（负 readable） | Buffer 内部 invariant 断言 + fuzz 半包测试 |

---

## 七、分阶段交付计划（总计 ~2 周）

### M1 · 基础设施层（0.3 周）
- [ ] `Timestamp / InetAddress / SocketUtil / SocketsOps`
- [ ] `Channel` + `tie` 弱引用
- [ ] `Poller` 抽象 + `EpollPoller`（LT）
- [ ] **验收**：单测覆盖 EPOLL_CTL ADD/MOD/DEL 全路径；events_ 扩容正确

### M2 · EventLoop 核心（0.4 周）
- [ ] `EventLoop`：loop/runInLoop/queueInLoop/doPendingFunctors/wakeup(eventfd)
- [ ] `TimerQueue`：绝对定时/周期/cancel/低漂移 resetTimerfd
- [ ] `EventLoopThread` + `EventLoopThreadPool`（round-robin）
- [ ] **验收**：Test_EventLoop 通过；8 个跨线程高频 runInLoop 无死锁；10ms 定时抖动 ≤±5ms（WSL2 基线）

### M3 · TCP 会话层（0.7 周）
- [ ] `Acceptor`：accept4 + EMFILE idleFd 兜底
- [ ] `Buffer`：双端索引 + readv 增强 + prependable
- [ ] `TcpConnection`：完整状态机 + shutdown/forceClose/sendInLoop 三路径
- [ ] `TcpServer`：newConnection/closeCallback/removeConnection 跨环生命周期闭环
- [ ] **验收**：echo 示例 1k 并发稳定；ASan + Helgrind 干净；Test_HalfFrame 粘包半包全覆盖

### M4 · 调优与压测（0.3 周）
- [ ] kPollTimeMs、TCP_NODELAY、SO_SNDBUF/SO_RCVBUF 微调开关化
- [ ] `echo_client_bench` ping-pong 基准 vs muduo 同环境对照
- [ ] WSL2 网络栈差异报告（注意 vNIC 中断合并导致延迟方差放大，记录不作为回归门槛）
- [ ] **验收指标**（对照 task.md §4.4，达成「不劣于 muduo」即可合入）：

| 指标 | 目标（vs muduo 版） |
|------|---------------------|
| 1000 连接 Echo QPS | ≥ 95% |
| p50 延迟 | ≤ 120% |
| p99 延迟 | ≤ 150% |
| 内存/万连接 | ≤ 110% |
| libnwl.a 大小 | ≤ muduo_net+base 的 70% |

### M5 · chatsystem 集成（0.3 周）
- [ ] [src/chatsystem/net/chatserver.hpp](src/chatsystem/net/chatserver.hpp)：muduo header → `nwl/*` 平替，字段不变
- [ ] [src/chatsystem/net/chatserver.cpp](src/chatsystem/net/chatserver.cpp)：分帧逻辑保留（API 同名直用），改由异步线程池继续承接业务
- [ ] 重跑 `tools/chat_stress.py` 全量脚本：登录/私聊/群聊/离线消息全回归
- [ ] Nginx stream 双节点集群下连跑 ≥1h 无泄漏、连接数曲线平稳
- [ ] **验收**：功能回归 100% 通过；gcda 覆盖率 nwl ≥85%；README/API 手册交付

---

## 八、风险清单与缓解

| # | 风险 | 缓解 |
|---|------|------|
| R1 | TcpConnection 生命周期竞态（最核心） | 三重保险：`enable_shared_from_this` + `tie()` + `closeCallback→removeConnection` 提升代机制；M3 专项 + ASan/Helgrind 双工具 |
| R2 | ET 模式漏读 | 默认 LT 不引入；ET 仅作为 M4+ 实验分支且独立 code review |
| R3 | EPOLLOUT 常开造成 busy loop | `disableWriting()` 在写完立即调用（TCP layer 自动降级）；handleWrite 已写完且有 Disconnecting 标记时收尾 |
| R4 | WSL2 与生产 Linux 行为差异 | 关键结论均在原生 Linux CI 上二次复核；WSL2 数据仅作参考不入门禁 |
| R5 | 与 muduo 符号冲突（过渡期共存） | nwl 全命名空间隔离 + 库名区分；链接目标二选一确保无双实现 |
| R6 | send 大块数据从 string 到 vector 拷贝两次 | 首发 `::write` 直写短路（见 5.4）+ 提供 `send(std::string&&)` 移动语义重载 |
| R7 | EventLoopThread 启动过早被主线程派发任务（loop 未进 poll） | startLoop 用 `std::promise<EventLoop*>` 保证返回时已在 poll 边缘 |

---

## 九、里程碑总览

```
Week1  ████████░░░░░░░░░░░░  M1 基础层(3d) + M2 EventLoop(4d)
Week1末———— ★ gate: 粘包半包 & 定时器 & 跨线程 单测全绿
Week2  ░░░░░░████████████░░  M3 TCP会话层(7d 含 ASan 回归)
Week3前中 -------- ★ gate: echo 1k并发 无泄漏 → 合入主干 pre-release
Week3  ░░░░░░░░░░░░░░░░████  M4 压测调优 + M5 chatsystem 集成回归
```

**结论**：本方案实现「自己造一个懂行版的 muduo」——零第三方依赖、纯 Linux syscall、回调签名与 chatsystem 完全兼容，M5 后 `chatsystem/net/chatserver` 仅替换 include 与构造参数名即可上线；且因保留了 `Poller` 抽象接口，后续无缝升级方案二（多后端）或叠加方案三（io_uring Proactor）实验分支而不破坏既有 API。
