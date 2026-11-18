#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <cstddef>

template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t max_capacity)
        : capacity_(max_capacity), closed_(false) {}

    // Non-blocking Leaky-Bucket Push:
    // Returns true if enqueued, or false immediately if the queue is at capacity q (dropped).
    bool try_push(T item) {
        std::lock_guard<std::mutex> lock(mtx_);
        
        if (closed_ || queue_.size() >= capacity_) {
            // Leaky-bucket overflow: drop item instead of blocking the gRPC network thread
            return false;
        }

        queue_.push(std::move(item));
        
        // Wake up ONE waiting consumer worker thread
        not_empty_cv_.notify_one();
        return true;
    }

    // Blocking Pop for Consumer Worker Threads:
    // Waits until an item is available or the queue is shut down.
    // Returns std::nullopt when closed and empty (graceful shutdown signal).
    std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(mtx_);

        // Predicate loop prevents spurious wakeups
        not_empty_cv_.wait(lock, [this]() {
            return !queue_.empty() || closed_;
        });

        if (queue_.empty() && closed_) {
            return std::nullopt;
        }

        T item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    // Graceful Shutdown: wakes all blocked consumer threads so they can exit cleanly
    void close() {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            closed_ = true;
        }
        not_empty_cv_.notify_all();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return queue_.size();
    }

private:
    const size_t capacity_;
    std::queue<T> queue_;
    mutable std::mutex mtx_;
    std::condition_variable not_empty_cv_;
    bool closed_;
};