# NetWorkLibrary 高性能网络框架可行性方案

> 目标：在 `src/NetWorkLibrary/` 目录下从零实现一套高性能网络框架，底层封装 `select / poll / epoll`，上层提供 `EventLoop / Reactor / Proactor` 等并发模型，替代/补充当前 `chatsystem` 对 muduo 库的依赖。

---

## 一、项目现状与约束

### 1.1 当前代码基线

| 模块 | 现状 | 说明 |
|------|------|------|
| `chatsystem/net/chatserver` | 基于 muduo `TcpServer + EventLoop` | Reactor + one-loop-per-thread |
| `src/threadpool/` | 已有 `ScheduleThreadPool / FixedThreadPool / CacheThreadPool` | 可复用为业务线程池 |
| `src/logSystem/` | 自研异步日志 `WT_LOG_*` | 可复用为框架日志组件 |
| `CMakeLists.txt` | C++17 静态库 + 可执行文件构建 | 需新增 `NetWorkLibrary` 子目录构建 |
| `src/NetWorkLibrary/` | 空目录 | 待实现 |

### 1.2 核心约束

1. **C++17 标准**：遵循项目现有构建配置
2. **Linux 平台优先**：epoll 为主，select/poll 作降级兼容
3. **可插拔 IO 多路复用**：统一 Poller 抽象，运行时/编译时可选后端
4. **线程安全**：跨线程调用 EventLoop 必须通过 `runInLoop / queueInLoop` 机制
5. **消息分帧**：沿用现有 4 字节大端长度前缀协议
6. **零拷贝友好**：Buffer 设计需避免冗余拷贝（参考 muduo Buffer）

---

## 二、可行性方案对比（4 种）

### 方案一：muduo 风格 One-Loop-Per-Thread Reactor（纯 epoll 实现）⭐推荐

#### 2.1.1 架构概览

```
┌─────────────────────────────────────────────────────────┐
│                    TcpServer (acceptor)                 │
│  ┌───────────────────────────────────────────────────┐  │
│  │            main EventLoop (accept loop)           │  │
│  │  listen sock ──epoll_wait──► accept ──round-robin │  │
│  └────────────────────┬──────────────────────────────┘  │
│                       │                                 │
│        ┌──────────────┼──────────────┐                  │
│        ▼              ▼              ▼                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ EventLoop│  │ EventLoop│  │ EventLoop│  IO Threads   │
│  │ +Poller  │  │ +Poller  │  │ +Poller  │  (N 个)       │
│  │ +TimerQueue         │  │  │ +ChannelMap│              │
│  └─────┬────┘  └─────┬────┘  └─────┬────┘              │
│        │              │              │                  │
│        └──────────────┼──────────────┘                  │
│                       ▼                                 │
│              ┌──────────────────┐                       │
│              │  WorkThreadPool  │  业务线程池（复用现有）│
│              │  (DB/Proto/业务) │                       │
│              └──────────────────┘                       │
└─────────────────────────────────────────────────────────┘
```

#### 2.1.2 核心模块设计

| 类名 | 职责 | 关键数据结构 |
|------|------|--------------|
| `Poller` (纯虚基类) | IO 多路复用统一抽象 | 统一 `poll(int timeoutMs)` 接口 |
| `EpollPoller : Poller` | epoll 封装 | `epoll_event events_[kInitEventListSize]`，`ChannelMap channels_` |
| `SelectPoller : Poller` | select 封装（可选） | `fd_set rfds/wfds/efds_`，最大 1024 fd |
| `PollPoller : Poller` | poll 封装（可选） | `vector<pollfd> pollfds_` |
| `Channel` | fd 事件绑定器 | fd_、events_、revents_、read/write/close/error 回调 |
| `EventLoop` | 事件循环核心 | `atomic looping_`、`Poller*`、`TimerQueue`、`pendingFunctors_` + mutex |
| `TimerQueue` | 定时器队列 | `set<TimerId, Timestamp>` 有序容器，timerfd 驱动 |
| `TcpServer` | 服务器入口 | Acceptor_、EventLoopThreadPool_、ConnectionMap_ |
| `Acceptor` | 接受连接 | listen_fd + Channel，执行 `accept4` 非阻塞 |
| `EventLoopThreadPool` | IO 线程池 | `vector<EventLoop*> loops_`，round-robin 分发 |
| `TcpConnection` | 连接封装 | 双端 Buffer、状态机 (Connected/Disconnecting/Disconnected) |
| `Buffer` | 应用层缓冲区 | `vector<char>` + readerIndex/writerIndex，prependable 预留区 |
| `EventLoopThread` | loop + thread 封装 | `thread + mutex + condvar` 同步启动 |

#### 2.1.3 关键机制

- **跨线程唤醒**：`EventLoop` 持有 `eventfd`，`queueInLoop` 写入 8 字节唤醒 `epoll_wait`
- **线程安全断言**：`EventLoop::assertInLoopThread()` 检查调用线程一致性
- **RAII 资源管理**：Socket fd 用 `Socket` 类包装，析构自动 close
- **ET/LT 模式**：默认 LT（与 muduo 一致，降低使用门槛），提供 ET 选项
- **连接关闭流程**：TcpConnection 状态机 + `shutdown()` 半关闭，处理 `POLLHUP`

#### 2.1.4 优劣分析

| 维度 | 评价 |
|------|------|
| **实现复杂度** | 中等（~3500 行核心代码） |
| **性能** | 高：epoll + 多线程 IO 可线性扩展 |
| **与现有项目契合度** | ⭐⭐⭐⭐⭐ 最高：chatserver 可几乎零改动切换 |
| **可维护性** | 高：架构清晰，与 muduo 同构，团队上手快 |
| **兼容性** | Linux 完美，select/poll 降级可覆盖 Unix 系 |

---

### 方案二：libevent 风格多后端 Poller + Reactor（统一抽象层）

#### 2.2.1 架构概览

```
┌───────────────────────────────────────────────────────┐
│                   Application Layer                    │
│         (TcpServer / HttpClient / User Code)           │
└───────────────────────────────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────┐
│                    EventLoop Core                      │
│  ┌─────────────────────────────────────────────────┐  │
│  │  Poller Interface (纯虚基类 vtable)              │  │
│  │    - dispatch(timeout)                           │  │
│  │    - add/mod/del(Channel*)                       │  │
│  └──────────┬──────────────┬──────────────┬─────────┘  │
│             ▼              ▼              ▼            │
│     ┌────────────┐  ┌────────────┐  ┌────────────┐    │
│     │EpollPoller │  │PollPoller  │  │SelectPoller│    │
│     │ (Linux)    │  │(POSIX 通用)│  │(老系统兼容)│    │
│     └────────────┘  └────────────┘  └────────────┘    │
│  ┌─────────────────────────────────────────────────┐  │
│  │    Signal Handling + Timer Queue + Async Queue   │  │
│  └─────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────┐
│              OS I/O Multiplexing Syscalls              │
└───────────────────────────────────────────────────────┘
```

#### 2.2.2 核心模块设计

在方案一基础上增加/强化：

| 类名 | 职责 | 设计要点 |
|------|------|----------|
| `PollerFactory` | Poller 工厂 | 运行时选择：`Poller::newDefaultPoller(EventLoop*)` |
| `EventBase` (EventLoop 增强) | 事件分发器 | 支持信号处理（signalfd）、优先级事件队列 |
| `Bufferevent` | 带缓冲的事件 | 封装 read/write Buffer + 水位回调（高/低水位线） |
| `EventListener` | 通用事件监听器 | 抽象 accept/connect 通用语义 |
| `DnsResolver` | 异步 DNS 解析 | getaddrinfo 提交到线程池，回调通知 |

#### 2.2.3 Poller 选择策略

```cpp
// 运行时探测优先级：epoll > poll > select
std::unique_ptr<Poller> Poller::newDefaultPoller(EventLoop* loop) {
#ifdef HAS_EPOLL
    return std::make_unique<EpollPoller>(loop);
#elif HAS_POLL
    return std::make_unique<PollPoller>(loop);
#else
    return std::make_unique<SelectPoller>(loop);
#endif
}
```

#### 2.2.4 优劣分析

| 维度 | 评价 |
|------|------|
| **实现复杂度** | 较高（~5000 行），多后端适配工作量大 |
| **性能** | 高（epoll 路径与方案一持平） |
| **与现有项目契合度** | ⭐⭐⭐⭐ 较高：API 略抽象，chatserver 需少量适配 |
| **可维护性** | 中高：抽象层增加一定理解成本 |
| **兼容性** | ⭐⭐⭐⭐⭐ 最佳：可移植到 macOS/BSD（kqueue 可扩展） |
| **灵活度** | 最高：运行时切换 Poller，便于性能对比测试 |

---

### 方案三：io_uring 异步 Proactor 模型（Linux 5.1+ 原生异步）

#### 2.3.1 架构概览

```
          Proactor 模式（异步完成通知）
┌─────────────────────────────────────────────────┐
│                TcpServer                        │
│  ┌───────────────────────────────────────────┐  │
│  │              ProactorLoop                  │  │
│  │  ┌─────────────────────────────────────┐  │  │
│  │  │      io_uring SQ/CQ Ring            │  │  │
│  │  │  SQ: submit accept/read/write       │  │  │
│  │  │  CQ: reap completion events         │  │  │
│  │  └────────────┬────────────────────────┘  │  │
│  │               ▼                           │  │
│  │  ┌─────────────────────────────────────┐  │  │
│  │  │   CompletionHandler Map             │  │  │
│  │  │   {user_data -> Handler}            │  │  │
│  │  └─────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────┘  │
│         无中间线程切换，系统调用异步化           │
└─────────────────────────────────────────────────┘
```

#### 2.3.2 核心模块设计

| 类名 | 职责 | 关键设计 |
|------|------|----------|
| `IoUring` | io_uring 封装 | `io_uring_setup / enter / register`，SQE/CQE 管理 |
| `ProactorLoop` | Proactor 事件循环 | 循环 `io_uring_wait_cqe`，分发完成事件 |
| `AsyncOp` | 异步操作基类 | `AcceptOp / ReadOp / WriteOp / ConnectOp`，带 user_data |
| `CompletionHandler` | 完成回调 | `void(AsyncOp*, int result, unsigned flags)` |
| `RingBuffer` | SQE 批量提交 | 合并多次 `io_uring_enter` 降低 syscall 开销 |
| `TcpServerProactor` | 服务器入口 | `multishot accept`（IORING_OP_ACCEPT 连发） |
| `TcpConnectionProactor` | 连接封装 | 读写均用 `IORING_OP_READV/WRITEV` + 分散聚集IO |

#### 2.3.3 关键特性

- **零拷贝 sendfile**：`IORING_OP_SPLICE / SENDMSG_ZC`（Linux 6.0+）
- **Multishot Accept**：一次 SQE 多次 accept CQE，降低提交频率
- **IORING_FEAT_FAST_POLL**：绕过传统 epoll 中间层
- **提供文件 IO**：除网络外，磁盘日志写入也可走 uring

#### 2.3.4 优劣分析

| 维度 | 评价 |
|------|------|
| **实现复杂度** | 高（~4000 行），io_uring API 细节多，内核行为差异 |
| **性能** | ⭐⭐⭐⭐⭐ 最高：高并发下 syscall 减少 30-50%，p99 延迟更低 |
| **与现有项目契合度** | ⭐⭐ 较低：Proactor vs Reactor API 差异大，chatserver 需重写 |
| **可维护性** | 中：需要团队理解异步完成模型，调试较难 |
| **兼容性** | 最低：要求 Linux >= 5.1，完整特性需 5.10+ |
| **适合场景** | 极致性能追求、团队有内核开发经验、明确部署环境 |

---

### 方案四：Hybrid Reactor-Proactor 混合模型（epoll + 线程池异步 IO）

#### 2.4.1 架构概览

```
         混合模型：Reactor 负责监听，Proactor 负责业务 IO
┌────────────────────────────────────────────────────────────┐
│                      Main Reactor                          │
│  (epoll LT 模式，负责 accept + 连接管理)                   │
│         │                                                  │
│         ▼                                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Sub Reactor Pool (N 个 IO 线程, epoll)             │    │
│  │  - 负责 fd 就绪事件通知（可读/可写）                 │    │
│  │  - 就绪后不直接 read/write，投递给 Proactor Pool    │    │
│  └────────────────────┬───────────────────────────────┘    │
│                       ▼                                    │
│  ┌────────────────────────────────────────────────────┐    │
│  │              Proactor Worker Pool (M 个线程)        │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐           │    │
│  │  │Worker #1 │ │Worker #2 │ │Worker #N │  ...       │    │
│  │  │ pread   │ │ readv   │ │ pwritev  │           │    │
│  │  │ sendfile│ │ recvmsg │ │ aio_*    │           │    │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘           │    │
│  │       └─────────────┼─────────────┘                 │    │
│  │                     ▼                               │    │
│  │           Completion Callback Queue                 │    │
│  │         (通过 eventfd 回写到 Sub Reactor)           │    │
│  └────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────┘
```

#### 2.4.2 核心模块设计

| 类名 | 职责 | 说明 |
|------|------|------|
| `MainReactor` | 监听 + 连接分发 | 单线程，Acceptor |
| `SubReactor` | IO 事件检测 | epoll_wait 检测 fd 就绪，不做 read/write |
| `ProactorPool` | 异步 IO 执行池 | 复用项目现有 WorkThreadPool |
| `AsyncTask` | 异步 IO 任务描述符 | `type(READ/WRITE/SENDFILE) + fd + iovec + handler` |
| `CompletionDispatcher` | 完成事件回填 | eventfd 通知 SubReactor 执行用户回调 |
| `HybridTcpConnection` | 混合连接封装 | 内部管理就绪态/读写态状态机 |

#### 2.4.3 优劣分析

| 维度 | 评价 |
|------|------|
| **实现复杂度** | 很高（~6000 行），双模型协同复杂 |
| **性能** | 较高：比纯 Reactor 好，但逊于原生 io_uring Proactor |
| **与现有项目契合度** | ⭐⭐⭐ 中等：线程池可复用，但连接语义改得多 |
| **可维护性** | 最低：状态空间爆炸，死锁/竞态调试难度高 |
| **兼容性** | 良好：Linux 2.6+ 即可运行 |
| **适合场景** | 有大量磁盘 IO 混合网络 IO 的场景（本项目不典型） |

---

## 三、方案推荐与实施路线

### 3.1 推荐结论

| 优先级 | 方案 | 适用阶段 | 推荐理由 |
|--------|------|----------|----------|
| **P0 必做** | **方案一：muduo 风格 Reactor** | 第一阶段 MVP | 与现有 chatsystem 完美对齐，风险最低，团队零学习成本迁移 |
| **P1 增强** | 方案二：多后端 Poller 抽象 | 第二阶段优化 | 在方案一基础上增量添加 select/poll，增加编译开关 |
| **P2 探索** | 方案三：io_uring Proactor | 第三阶段特性 | 作为实验分支，对比性能后决定是否生产启用 |
| **不推荐** | 方案四：Hybrid 混合 | — | 复杂度 > 收益，与当前纯网络业务匹配度低 |

### 3.2 推荐实施路径（方案一为主干）

```
阶段 1（~1 周）：基础设施
├── Poller 抽象 + EpollPoller 实现（含单元测试）
├── Channel + EventLoop（含 eventfd 跨线程唤醒）
├── TimerQueue（timerfd + 红黑树/set 排序）
└── Socket + InetAddress 地址封装

阶段 2（~1 周）：TCP 核心
├── Acceptor（非阻塞 accept4）
├── EventLoopThread + EventLoopThreadPool
├── TcpConnection（双 Buffer + 状态机）
└── TcpServer（整体串联）

阶段 3（~3 天）：集成与验证
├── 替换 chatsystem/chatserver 依赖（从 muduo → NetWorkLibrary）
├── 压力测试对比现有 muduo 版本（QPS / 延迟 / CPU 占用）
├── 压测脚本复用 tools/chat_stress.py
└── 补齐文档：README + API 手册

阶段 4（可选，~1 周）：增量增强
├── PollPoller + SelectPoller 降级实现
├── PollerFactory 运行时选择
├── ET 模式性能调优选项
└── Buffer::readFd 用 iovec + readv 优化一次拷贝
```

### 3.3 预期目录结构

```
src/NetWorkLibrary/
├── CMakeLists.txt                      # 构建：libnetwork.a
├── README.md                           # 设计文档 + API 说明
├── include/                            # 对外头文件（用户 include 路径）
│   ├── EventLoop.hpp
│   ├── Channel.hpp
│   ├── Poller.hpp
│   ├── TimerId.hpp
│   ├── TcpServer.hpp
│   ├── TcpConnection.hpp
│   ├── Buffer.hpp
│   ├── InetAddress.hpp
│   ├── Socket.hpp
│   └── Callbacks.hpp                    # typedef ConnectionCallback / MessageCallback 等
├── src/                                # 内部实现
│   ├── poller/
│   │   ├── EpollPoller.hpp / .cpp
│   │   ├── PollPoller.hpp / .cpp      # 可选
│   │   └── SelectPoller.hpp / .cpp    # 可选
│   ├── EventLoop.cpp
│   ├── Channel.cpp
│   ├── TimerQueue.hpp / .cpp
│   ├── Acceptor.hpp / .cpp
│   ├── EventLoopThread.hpp / .cpp
│   ├── EventLoopThreadPool.hpp / .cpp
│   ├── TcpConnection.cpp
│   ├── TcpServer.cpp
│   ├── Buffer.cpp
│   ├── Socket.cpp
│   ├── InetAddress.cpp
│   └── SocketsOps.hpp / .cpp           # ::bind/listen/accept 等 C 函数封装
└── tests/                              # 单元测试
    ├── Test_EventLoop.cpp
    ├── Test_TimerQueue.cpp
    ├── Test_Buffer.cpp
    ├── Test_TcpServer.cpp              # 与 muduo 版本的 echo 对比
    └── CMakeLists.txt
```

### 3.4 CMake 集成建议

```cmake
# src/NetWorkLibrary/CMakeLists.txt
add_library(network STATIC
    src/EventLoop.cpp
    src/Channel.cpp
    src/TimerQueue.cpp
    src/poller/EpollPoller.cpp
    src/Acceptor.cpp
    src/EventLoopThread.cpp
    src/EventLoopThreadPool.cpp
    src/TcpConnection.cpp
    src/TcpServer.cpp
    src/Buffer.cpp
    src/Socket.cpp
    src/InetAddress.cpp
    src/SocketsOps.cpp
)

target_include_directories(network PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(network PRIVATE
    muduo_log  # 复用现有日志库 WT_LOG_*
    pthread
)
```

---

## 四、关键技术选型细节

### 4.1 Buffer 设计

```cpp
class Buffer {
    static constexpr size_t kCheapPrepend = 8;
    static constexpr size_t kInitialSize  = 1024;
    // 内存布局: [prepend 8B][readable...][writable...]
    // peek() = data_ + readerIndex_
    // readFd(fd, &savedErrno) 用 readv + stack 临时 buf 避免多次扩容
};
```

### 4.2 回调类型定义

```cpp
// Callbacks.hpp —— 与 muduo 保持签名一致，便于迁移
using TimerCallback = std::function<void()>;
using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
using MessageCallback = std::function<void(const TcpConnectionPtr&, Buffer*, Timestamp)>;
using WriteCompleteCallback = std::function<void(const TcpConnectionPtr&)>;
using HighWaterMarkCallback = std::function<void(const TcpConnectionPtr&, size_t)>;
```

### 4.3 线程安全策略

| 组件 | 所属线程 | 跨线程访问方式 |
|------|----------|----------------|
| EventLoop::loop() | IO 线程独占 | 禁止，用 `assertInLoopThread()` 检查 |
| EventLoop::runInLoop() | 任意线程可调用 | loop 内直接执行；否则 `queueInLoop + wakeup` |
| TcpConnection 回调 | 所属 SubReactor IO 线程 | 回调内可直接操作，业务处理投递到线程池 |
| TcpServer 构造/析构 | 主线程 | 禁止跨线程析构，`std::enable_shared_from_this` 延长生命周期 |

### 4.4 压测对比指标（验收标准）

| 指标 | 目标（对比 muduo 版本） |
|------|-------------------------|
| 1000 连接 Echo QPS | ≥ muduo 的 95% |
| 10w 连接内存占用 | ≤ muduo 的 110% |
| p50 延迟 | ≤ muduo 的 120% |
| p99 延迟 | ≤ muduo 的 150% |
| 编译体积（libnetwork.a） | ≤ muduo_net.a 的 70% |

---

## 五、风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| ET 模式事件漏处理 | 连接 hang 住，数据丢失 | 默认 LT，ET 加充分单测覆盖 EAGAIN 分支 |
| TcpConnection 生命周期 race | use-after-free core dump | 全员 `shared_ptr`，`enable_shared_from_this`，析构打日志追踪 |
| 跨线程唤醒竞态 | pendingFunctors_ 丢失 | mutex 保护 + wakeup() 必执行，参考 muduo 官方实现 |
| epoll_ctl 操作 fd 已关闭 | ENOENT 报错 | Channel::update/remove 前检查 `fd_ < 0` 早返回 |
| 消息分包边界 bug | 解析失败/粘包 | 单测覆盖：半包、跨两包、多包粘连、超大包 |

---

## 六、结论

**建议从方案一（muduo 风格 one-loop-per-thread Reactor）启动**，原因：

1. **迁移零风险**：当前 `chatserver.hpp` 可直接替换 `muduo/net/TcpServer.h` → `NetWorkLibrary/TcpServer.hpp`，回调签名完全兼容；
2. **代码量可控**：核心 ~3500 行，2 周内可完成并通过压测；
3. **后续可演进**：方案一代码结构预留了 Poller 抽象接口，后续可无缝升级方案二/三；
4. **团队认知一致**：成员已掌握 muduo 思想，相当于"造一个自己懂的 muduo"，无学习曲线断层。
