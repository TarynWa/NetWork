// 冒烟测试（plan.md M2 验收项）：验证跨线程 runInLoop/queueInLoop 投递、
// eventfd 唤醒打断 epoll_wait 以及干净退出。
// 注意遵守 one-loop-per-thread：EventLoop 在其构造线程上执行 loop()/析构，
// 其他线程只能通过 runInLoop/queueInLoop 投递任务。
#include "Logger.hpp"      // WT_LOG_* 日志桥接（lib::muduo_log）
#include "nwl/EventLoop.hpp"
#include <chrono>
#include <cstdio>
#include <thread>

using namespace nwl;
using namespace std::chrono_literals;

int main() {
    EventLoop loop;                       // 属于主线程

    std::thread outsider([&loop] {        // 外部线程仅投递
        std::this_thread::sleep_for(50ms);
        loop.runInLoop([] {
            WT_LOG_INFO << "[ok] runInLoop: auto-redirected to loop thread";
        });
        loop.queueInLoop([] {
            WT_LOG_INFO << "[ok] queueInLoop: via pending batch";
        });
        std::this_thread::sleep_for(50ms);
        loop.queueInLoop([&loop] { loop.quit(); });   // 触发跨线程退出
    });

    loop.loop();                          // 阻塞至 quit
    outsider.join();

    std::printf("SMOKE PASS\n");
    return 0;
}
