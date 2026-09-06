# NetWorkLibrary (NWL)

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![C++](https://img.shields.io/badge/C++-17-blue.svg)](https://en.cppreference.com/w/cpp/17)
[![Platform](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](https://www.kernel.org/doc/html/latest/networking/epoll.html)

一个高性能的 C++ 网络库，采用 **One-Loop-Per-Thread** Reactor 模式，基于 Linux epoll 实现零依赖的异步网络框架。

## 🌟 特性

- **零第三方依赖** - 纯 POSIX/Linux syscall 实现，无需安装任何第三方包
- **高性能** - 基于 epoll LT 模式，支持高并发连接处理
- **线程安全** - One-Loop-Per-Thread 架构，跨线程通信通过事件队列
- **定时器支持** - 集成 TimerQueue，支持一次性/周期性定时任务
- **智能缓冲区** - 双端索引 Buffer 设计，支持零拷贝读取
- **muduo 兼容** - API 设计与 muduo 库同构，便于现有项目迁移
- **C++17 标准** - 使用现代 C++ 特性，代码简洁高效

## 📁 项目结构

```
NetWork/
├── CMakeLists.txt          # 构建配置
├── include/nwl/           # 公共头文件
│   ├── Callbacks.hpp      # 回调类型定义
│   ├── Channel.hpp        # IO 事件通道
│   ├── EventLoop.hpp      # 事件循环核心
│   ├── Poller.hpp         # IO 多路复用抽象
│   ├── TcpServer.hpp      # TCP 服务器
│   ├── TcpConnection.hpp  # TCP 连接管理
│   ├── Buffer.hpp         # 智能缓冲区
│   ├── InetAddress.hpp    # 网络地址封装
│   ├── Socket.hpp         # Socket 封装
│   └── TimerId.hpp        # 定时器 ID
├── src/
│   ├── core/              # 核心组件
│   │   ├── EventLoop.cpp
│   │   ├── Channel.cpp
│   │   └── TimerQueue.cpp
│   ├── poller/            # IO 多路复用实现
│   │   ├── Poller.cpp
│   │   └── EpollPoller.cpp
│   ├── net/               # 网络层组件
│   │   ├── TcpServer.cpp
│   │   ├── TcpConnection.cpp
│   │   ├── Acceptor.cpp
│   │   ├── EventLoopThread.cpp
│   │   └── EventLoopThreadPool.cpp
│   └── util/              # 工具类
│       ├── SocketsOps.cpp
│       └── ...
└── examples/              # 示例程序
    ├── echo_server_nwl.cpp      # Echo 服务器
    ├── echo_bench_client.cpp    # 压测客户端
    └── smoke_eventloop.cpp      # 简单事件循环测试
```

## 🚀 快速开始

### 构建项目

```bash
cd NetWork
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### 运行示例

#### Echo 服务器

```bash
# 使用默认端口 (9097) 和 4 个 IO 线程
./nwl_echo_server

# 指定端口和线程数
./nwl_echo_server 8080 8
```

#### 压测客户端

```bash
# 连接到默认服务器的压测
./nwl_bench_client

# 连接到指定服务器
./nwl_bench_client 127.0.0.1 8080
```

## 💡 使用示例

### 简单的 Echo 服务器

```cpp
#include "nwl/EventLoop.hpp"
#include "nwl/TcpServer.hpp"
#include "Logger.hpp"

using namespace nwl;

int main() {
    // 忽略 SIGPIPE 信号，防止向已断开的 socket 写入导致进程退出
    ::signal(SIGPIPE, SIG_IGN);
    
    // 创建主事件循环
    EventLoop loop;
    
    // 创建 TCP 服务器
    TcpServer server(&loop, InetAddress(9097), "echo_server");
    server.setThreadNum(4);  // 设置 4 个 IO 线程
    
    // 设置连接回调
    server.setConnectionCallback([](const TcpConnPtr& conn) {
        if (conn->connected()) {
            WT_LOG_INFO << "Connection up: " << conn->name() << " "
                       << conn->peerAddress().toIpPort();
        } else {
            WT_LOG_INFO << "Connection down: " << conn->name();
        }
    });
    
    // 设置消息回调（简单的 echo 功能）
    server.setMessageCallback([](const TcpConnPtr& conn, Buffer* buf, Timestamp) {
        conn->send(buf);  // 原样回发
        buf->retrieveAll();  // 清空缓冲区
    });
    
    // 启动服务器
    server.start();
    
    // 进入事件循环
    loop.loop();
    
    return 0;
}
```

### 使用定时器

```cpp
#include "nwl/EventLoop.hpp"

using namespace nwl;

int main() {
    EventLoop loop;
    
    // 5 秒后执行一次性任务
    loop.runAfter([]() {
        WT_LOG_INFO << "Timer fired after 5 seconds";
    }, 5.0);
    
    // 每秒执行周期性任务
    TimerId timer = loop.runEvery([]() {
        WT_LOG_INFO << "Periodic timer";
    }, 1.0);
    
    // 10 秒后取消定时器
    loop.runAfter([timerId = timer.id]() {
        loop.cancelTimer(timerId);
    }, 10.0);
    
    loop.loop();
    return 0;
}
```

## 🏗️ 架构设计

### One-Loop-Per-Thread 模式

```
┌──────────────────────────────────────────────────────────────┐
│  应用层: chatservice / echo demo                              │
│           onConnection(TcpConnectionPtr)                     │
│           onMessage(TcpConnectionPtr, Buffer*, Timestamp)     │
├──────────────────────────────────────────────────────────────┤
│  会话层: TcpServer ── Acceptor(listenfd+Channel)              │
│                  │  round-robin 分发                          │
│  ┌────────────▼────────────┬──────────────────┐              │
│  │ SubReactor #0           │ SubReactor #N    │              │
│  │ EventLoop               │ EventLoop        │              │
│  │ ├ EpollPoller (LT)      │ ├ ...            │              │
│  │ ├ TimerQueue(timerfd)   │                  │              │
│  │ └ wakeup eventfd        │                  │              │
│  └────┬────────────────────┴──────┬───────────┘              │
│       │ TcpConnection(inputBuf/outputBuf)                      │
├───────▼──────────────────────────────────────────────────────┤
│  业务卸载层: ScheduleThreadPool::Submit(DB查询/Proto解析)     │
├──────────────────────────────────────────────────────────────┤
│  OS 层: epoll_wait(LT) / timerfd / eventfd / accept4          │
└──────────────────────────────────────────────────────────────┘
```

### 核心组件

| 组件 | 职责 |
|------|------|
| **EventLoop** | 事件循环核心，每个线程一个实例，负责 IO 事件分发和定时任务 |
| **Channel** | 文件描述符封装，注册感兴趣的事件 |
| **Poller/EpollPoller** | IO 多路复用的抽象接口和 epoll 实现 |
| **TcpServer** | TCP 服务器，管理监听 socket 和连接分发 |
| **TcpConnection** | TCP 连接管理，封装 socket 和缓冲区 |
| **Buffer** | 双端缓冲区，支持高效的数据读写 |
| **TimerQueue** | 定时器管理，基于 timerfd 实现 |

## 🔧 核心概念

### 线程模型

- **Main Loop**: 主线程的事件循环，仅负责 accept 新连接
- **IO Threads**: 工作线程池，每个线程运行一个 EventLoop，处理已建立连接的 IO 事件
- **跨线程通信**: 通过 `runInLoop()` 和 `queueInLoop()` 机制安全地跨线程调用

### 回调类型

```cpp
// 连接状态变化回调
using ConnectionCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;

// 消息到达回调
using MessageCallback = std::function<void(const std::shared_ptr<TcpConnection>&, 
                                           Buffer*, Timestamp)>;

// 写完成回调
using WriteCompleteCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;

// 高水位回调
using HighWaterMarkCallback = std::function<void(const std::shared_ptr<TcpConnection>&, 
                                                 size_t)>;
```

## 📊 性能特点

- **零拷贝**: Buffer 设计支持 readv 一次性读取，减少数据拷贝
- **事件驱动**: 基于 epoll 的事件驱动模型，避免轮询开销
- **线程局部存储**: EventLoop 通过 thread_local 确保线程独占
- **无锁设计**: 同一线程内操作无锁，跨线程通过队列通信

## 🔨 构建配置

### 依赖要求

- CMake >= 3.14
- GCC >= 7.0 (支持 C++17)
- Linux 内核 >= 2.6 (epoll 支持)
- 线程库 (pthread)

### 集成到现有项目

在 CMakeLists.txt 中添加：

```cmake
# 添加 NWL 子目录
add_subdirectory(NetWork)

# 链接到你的目标
target_link_libraries(your_target PRIVATE nwl)
```

## 📝 开发规范

- 遵循 C++17 标准
- 使用 RAII 管理资源
- 避免跨线程直接调用 EventLoop 方法
- 使用智能指针管理对象生命周期
- 保持与 muduo API 的兼容性

## 🐛 调试建议

1. 启用详细日志：
   ```cpp
   wangt::Logger::setLogLevel(wangt::LogLevel::DEBUG);
   ```

2. 使用 GDB 调试多线程：
   ```bash
   gdb -ex "set pagination off" -ex "run" ./nwl_echo_server
   ```

3. 监控文件描述符：
   ```bash
   watch -n 1 'ls -l /proc/$(pidof nwl_echo_server)/fd | wc -l'
   ```

## 📚 参考资料

- [muduo 网络库](https://github.com/chenshuo/muduo)
- [Linux epoll 官方文档](https://man7.org/linux/man-pages/man7/epoll.7.html)
- [POSIX 线程编程](https://computing.llnl.gov/tutorials/pthreads/)
- [C++17 特性](https://en.cppreference.com/w/cpp/17)

## 📄 许可证

本项目采用 MIT 许可证。详见 LICENSE 文件。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📧 联系方式

如有问题或建议，请通过 GitHub Issues 联系。

---

**NetWorkLibrary** - 简单、高效、零依赖的高性能网络库
