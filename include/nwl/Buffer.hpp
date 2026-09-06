#ifndef NWL_BUFFER_HPP
#define NWL_BUFFER_HPP
// Buffer：双端索引缓冲区，readFd 用 readv+栈上备用块一次读尽（plan.md §3.6）
// API 与 chatsystem/net/chatserver.cpp 分帧逻辑调用点同名对齐：
//   readableBytes() / peek() / retrieve(n) / retrieveAsString(n) / append(...)
#include <sys/uio.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

namespace nwl {

class Buffer {
public:
    static constexpr size_t kCheapPrepend = 8;
    static constexpr size_t kInitialSize  = 1024;

    explicit Buffer(size_t initialSize = kInitialSize)
        : buffer_(kCheapPrepend + initialSize),
          readerIndex_(kCheapPrepend),
          writerIndex_(kCheapPrepend) {}

    // ---- 尺寸查询 ----
    size_t readableBytes() const { return writerIndex_ - readerIndex_; }
    size_t writableBytes() const { return buffer_.size() - writerIndex_; }

    const char* peek() const { return begin() + readerIndex_; }

    // ---- 消费 ----
    void retrieve(size_t len) {
        if (len < readableBytes()) {
            readerIndex_ += len;
        } else {
            retrieveAll();
        }
    }

    void retrieveAll() {
        readerIndex_ = kCheapPrepend;
        writerIndex_ = kCheapPrepend;
    }

    std::string retrieveAllAsString() { return retrieveAsString(readableBytes()); }

    std::string retrieveAsString(size_t len) {
        len = len < readableBytes() ? len : readableBytes();
        std::string result(peek(), static_cast<size_t>(len));
        retrieve(len);
        return result;
    }

    // ---- 写入 ----
    char* beginWrite() { return begin() + writerIndex_; }
    const char* beginWrite() const { return begin() + writerIndex_; }

    void hasWritten(size_t len) { writerIndex_ += len; }

    void append(const std::string& str) { append(str.data(), str.size()); }

    void append(const char* data, size_t len) {
        ensureWritableBytes(len);
        std::memcpy(beginWrite(), data, len);
        hasWritten(len);
    }

    void prepend(const void* data, size_t len) {
        if (readerIndex_ < len) return;             // prependable 区不足（预留空间保护）
        readerIndex_ -= len;
        std::memcpy(begin() + readerIndex_, data, len);
    }

    void ensureWritableBytes(size_t len) {
        if (writableBytes() < len) {
            makeSpace(len);
        }
    }

    /// 返回读到的字节数；errno 传出以便调用方区分 EAGAIN/ECONNRESET
    ssize_t readFd(int fd, int* savedErrno);

private:
    char* begin() { return buffer_.data(); }
    const char* begin() const { return buffer_.data(); }

    void makeSpace(size_t len) {
        if (writableBytes() + readerIndex_ - kCheapPrepend >= len + kCheapPrepend) {
            // 前部空闲足够：memmove 前移压实即可
            size_t readable = readableBytes();
            std::memmove(begin() + kCheapPrepend, begin() + readerIndex_, readable);
            readerIndex_ = kCheapPrepend;
            writerIndex_ = readerIndex_ + readable;
        } else {
            buffer_.resize(writerIndex_ + len);
        }
    }

    std::vector<char> buffer_;
    size_t readerIndex_;
    size_t writerIndex_;
};

inline ssize_t Buffer::readFd(int fd, int* savedErrno) {
    char extrabuf[65536];                                  // 栈上备用块避免扩容拷贝
    struct iovec vec[2];
    const size_t writable = writableBytes();
    vec[0].iov_base = beginWrite();
    vec[0].iov_len  = writable;
    vec[1].iov_base = extrabuf;
    vec[1].iov_len  = sizeof extrabuf;
    const int iovcnt = (writable < sizeof extrabuf) ? 2 : 1;

    const ssize_t n = ::readv(fd, vec, iovcnt);
    if (n <= 0) {
        *savedErrno = errno;
    } else if (static_cast<size_t>(n) <= writable) {
        writerIndex_ += n;                                 // 第一块就装下了
    } else {
        writerIndex_ = buffer_.size();
        append(extrabuf, static_cast<size_t>(n) - writable);   // 增量扩容吸收溢出
    }
    return n;
}

} // namespace nwl

#endif // NWL_BUFFER_HPP
