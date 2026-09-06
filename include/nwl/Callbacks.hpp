#ifndef NWL_CALLBACKS_HPP
#define NWL_CALLBACKS_HPP
// 回调类型定义 —— 与 muduo / chatsystem 现有签名同构（见 plan.md §5.1）
#include <functional>
#include <memory>
#include "Timestamp.hpp"

namespace nwl {

// 复用 logSystem 的时间戳，避免重复实现
using Timestamp = wangt::Timestamp;

class Channel;
class TcpConnection;

// 事件循环投递的无参闭包
using Functor = std::function<void()>;
// 定时器回调（TimerQueue 阶段启用）
using TimerCallback = Functor;

using ConnectionCallback    = std::function<void(const std::shared_ptr<TcpConnection>&)>;
using MessageCallback       = std::function<void(const std::shared_ptr<TcpConnection>&, class Buffer*, Timestamp)>;
using WriteCompleteCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;
using HighWaterMarkCallback = std::function<void(const std::shared_ptr<TcpConnection>&, size_t)>;
using CloseCallback         = std::function<void(const std::shared_ptr<TcpConnection>&)>;

} // namespace nwl

#endif // NWL_CALLBACKS_HPP
