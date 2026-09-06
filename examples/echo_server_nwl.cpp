// Echo 服务端示例：演示 NetWorkLibrary 全栈用法（plan.md §5.5）
// 用法: ./nwl_echo_server [port] [io_threads]
#include <signal.h>
#include "Logger.hpp"
#include "nwl/EventLoop.hpp"
#include "nwl/TcpServer.hpp"

using namespace nwl;

int main(int argc, char* argv[]) {
    ::signal(SIGPIPE, SIG_IGN);          // 防止向已断开 socket 写入触发进程退出
    wangt::Logger::setFlush([] { ::fflush(stdout); });   // 默认 flush 为空操作，崩溃时缓冲日志会丢
    const uint16_t port = argc > 1 ? static_cast<uint16_t>(std::atoi(argv[1])) : 9097;
    const int threads    = argc > 2 ? std::atoi(argv[2]) : 4;

    EventLoop loop;
    TcpServer server(&loop, InetAddress(port), "echo_server", TcpServer::Option::kNoReusePort);
    server.setThreadNum(threads);

    server.setConnectionCallback([](const TcpConnPtr& conn) {
        if (conn->connected()) {
            WT_LOG_INFO << "conn up  [" << conn->name() << "] "
                        << conn->peerAddress().toIpPort();
        } else {
            WT_LOG_INFO << "conn down [" << conn->name() << "]";
        }
    });

    // 收到什么回什么：原样回写给发送方（演示 send 双线程安全性）
    server.setMessageCallback([](const TcpConnPtr& conn, Buffer* buf, Timestamp t) {
        const std::string payload = buf->retrieveAllAsString();
        (void)t;
        conn->send(payload);
    });

    server.start();
    loop.loop();
    return 0;
}
