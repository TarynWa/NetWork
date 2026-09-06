// Ping-pong 压测客户端（plan.md M4 验收工具）
// 用法: ./nwl_bench_client [ip] [port] [workerThreads] [connsPerWorker] [msgsPerConn]
// 每条消息携带序号前缀校验回显一致性；统计总吞吐与平均 RTT
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr int kConnectRetry = 30;        // 起服务端前的就绪等待：重试 30×0.2s
constexpr int kMsgPrefixLen = 16;        // "00000001|xxxxxx" 前缀校验区

int connectRetry(const char* ip, uint16_t port) {
    for (int i = 0; i < kConnectRetry; ++i) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return -1;
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = ::htons(port);
        ::inet_pton(AF_INET, ip, &addr.sin_addr);
        if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof addr) == 0) {
            int on = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);
            return fd;
        }
        ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return -1;
}

void workerRun(const char* ip, uint16_t port, int conns, long msgs,
               const std::string& tag,
               std::atomic<long>& okCount, std::atomic<int>& errCount) {
    std::vector<int> fds(conns);
    for (int i = 0; i < conns; ++i) {
        fds[i] = connectRetry(ip, port);
        if (fds[i] < 0) { errCount.fetch_add(conns - i); return; }
    }

    char buf[512];
    for (long m = 0; m < msgs; ++m) {
        for (int c = 0; c < conns; ++c) {
            // 消息: "%08d|%s#%c" —— 回显后逐字节比对
            const int len = std::snprintf(buf, sizeof buf, "%08ld|%s", m, tag.c_str());
            if (::send(fds[c], buf, len, MSG_NOSIGNAL) != len) {
                errCount.fetch_add(1); continue;
            }
            char echo[512];
            const ssize_t n = ::recv(fds[c], echo, sizeof echo, 0);
            if (n == len && ::memcmp(echo, buf, len) == 0) {
                okCount.fetch_add(1);
            } else {
                errCount.fetch_add(1);
            }
        }
    }
    for (int c = 0; c < conns; ++c) ::close(fds[c]);
}

} // namespace

int main(int argc, char* argv[]) {
    ::signal(SIGPIPE, SIG_IGN);
    const char* ip           = argc > 1 ? argv[1] : "127.0.0.1";
    const uint16_t port      = argc > 2 ? static_cast<uint16_t>(std::atoi(argv[2])) : 9097;
    const int workers        = argc > 3 ? std::atoi(argv[3]) : 2;
    const int conns          = argc > 4 ? std::atoi(argv[4]) : 8;
    const long msgs          = argc > 5 ? std::atol(argv[5]) : 500;

    std::atomic<long> ok{0};
    std::atomic<int>  err{0};

    const auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> pool;
    for (int w = 0; w < workers; ++w) {
        pool.emplace_back(workerRun, ip, port, conns, msgs,
                          "w" + std::to_string(w), std::ref(ok), std::ref(err));
    }
    for (auto& t : pool) t.join();
    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t0).count();

    const long total = static_cast<long>(workers) * conns * msgs;
    std::printf("bench: sent=%ld ok=%ld err=%d elapsed=%.3fs qps=%.0f\n",
                total, ok.load(), err.load(), elapsed,
                elapsed > 0 ? ok.load() / elapsed : 0.0);

    const bool pass = (err.load() == 0 && ok.load() == total);
    std::printf(pass ? "BENCH PASS\n" : "BENCH FAIL\n");
    return pass ? 0 : 1;
}
