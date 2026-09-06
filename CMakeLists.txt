cmake_minimum_required(VERSION 3.14)
project(nwl LANGUAGES CXX)

# NetWorkLibrary：muduo 风格 One-Loop-Per-Thread Reactor（plan.md 方案一）
# 覆盖 M1~M3：Channel/Poller/EpollPoller/EventLoop+TimerQueue/Acceptor/Buffer/
#            TcpConnection/TcpServer/EventLoopThreadPool
set(NWL_SOURCES
    src/core/Channel.cpp
    src/core/EventLoop.cpp
    src/core/TimerQueue.cpp
    src/poller/Poller.cpp
    src/poller/EpollPoller.cpp
    src/net/Acceptor.cpp
    src/net/EventLoopThread.cpp
    src/net/EventLoopThreadPool.cpp
    src/net/TcpConnection.cpp
    src/net/TcpServer.cpp
    src/net/InetAddress.cpp
    src/net/Socket.cpp
    src/util/SocketsOps.cpp
)

add_library(nwl STATIC ${NWL_SOURCES})

target_include_directories(nwl
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_features(nwl PUBLIC cxx_std_17)

find_package(Threads REQUIRED)
# 复用 logSystem 的 WT_LOG_* 日志桥接（plan.md §一 可复用资产）
target_link_libraries(nwl PUBLIC Threads::Threads lib::muduo_log)

add_library(lib::nwl ALIAS nwl)

# 冒烟示例（M2 验收）
add_executable(nwl_smoke examples/smoke_eventloop.cpp)
target_link_libraries(nwl_smoke PRIVATE nwl)

# Echo 服务端 + 压测客户端（M3/M4 验收）
add_executable(nwl_echo_server examples/echo_server_nwl.cpp)
target_link_libraries(nwl_echo_server PRIVATE nwl)

add_executable(nwl_bench_client examples/echo_bench_client.cpp)
target_link_libraries(nwl_bench_client PRIVATE Threads::Threads)
