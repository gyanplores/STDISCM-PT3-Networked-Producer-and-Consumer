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

    // Step 1 of an upload: claim a place in the queue BEFORE the video is received.
    // The check and the claim happen under one lock, so two uploads can never
    // both take the last free place. Returns false immediately if there is no
    // room (leaky bucket: the upload is turned away, nothing blocks).
    // A claimed place counts as "used" until commit() or cancel_reservation().
    bool try_reserve() {
        std::lock_guard<std::mutex> lock(mtx_);
        if (closed_ || queue_.size() + reserved_ >= capacity_) {
            return false;
        }
        ++reserved_;
        return true;
    }

    // Step 2a: the video arrived fine. Turn the claimed place into a real item.
    // Always succeeds (the place was already ours), so it never drops.
    void commit(T item) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            --reserved_;
            queue_.push(std::move(item));
        }
        // Wake up ONE waiting consumer worker thread
        not_empty_cv_.notify_one();
    }

    // Step 2b: the upload failed (corrupted, cut off, duplicate...). Give the place back.
    void cancel_reservation() {
        std::lock_guard<std::mutex> lock(mtx_);
        --reserved_;
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
    size_t reserved_ = 0;   // places claimed by uploads that are still in transit
    std::queue<T> queue_;
    mutable std::mutex mtx_;
    std::condition_variable not_empty_cv_;
    bool closed_;
};