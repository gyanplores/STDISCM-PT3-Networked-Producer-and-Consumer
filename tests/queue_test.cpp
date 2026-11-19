// Small self-check for BoundedQueue (no gRPC needed).
// Build:  cmake --build build --target queue_test      Run:  ./build/queue_test
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>
#include "BoundedQueue.hpp"

static int failures = 0;
#define CHECK(cond) do { if (!(cond)) { std::cout << "FAIL line " << __LINE__ << ": " #cond "\n"; ++failures; } } while (0)

int main() {
    // 1. A queue of 2 hands out exactly 2 places, then turns the 3rd away.
    {
        BoundedQueue<int> q(2);
        CHECK(q.try_reserve());
        CHECK(q.try_reserve());
        CHECK(!q.try_reserve());
        std::cout << "1. capacity respected\n";
    }
    // 2. A cancelled reservation gives the place back.
    {
        BoundedQueue<int> q(1);
        CHECK(q.try_reserve());
        CHECK(!q.try_reserve());
        q.cancel_reservation();
        CHECK(q.try_reserve());
        std::cout << "2. cancel frees the place\n";
    }
    // 3. A committed item still occupies its place until a worker pops it.
    {
        BoundedQueue<int> q(1);
        CHECK(q.try_reserve());
        q.commit(42);
        CHECK(q.size() == 1);
        CHECK(!q.try_reserve());
        auto item = q.pop();
        CHECK(item && *item == 42);
        CHECK(q.try_reserve());
        std::cout << "3. place is freed on pop (Option B)\n";
    }
    // 4. close() wakes a blocked worker, and pop() then returns nothing.
    {
        BoundedQueue<int> q(1);
        std::atomic<bool> got_nullopt{false};
        std::thread worker([&] { got_nullopt = !q.pop().has_value(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        q.close();
        worker.join();
        CHECK(got_nullopt);
        CHECK(!q.try_reserve());
        std::cout << "4. close wakes workers and stops new reservations\n";
    }
    // 5. Stress: 16 uploaders x 1000 tries against capacity 5 with 2 workers.
    //    Places handed out must never exceed 5 at once, and every committed item must be popped.
    //    (The test's own counter is decremented just AFTER pop() returns, so it can read up
    //    to 'number of workers' too high; the allowed limit below includes that slack.)
    {
        const size_t cap = 5;
        BoundedQueue<int> q(cap);
        std::atomic<int> in_use{0}, max_in_use{0}, committed{0}, popped{0};
        std::vector<std::thread> workers, uploaders;
        for (int w = 0; w < 2; ++w)
            workers.emplace_back([&] { while (q.pop()) { --in_use; ++popped; } });
        for (int u = 0; u < 16; ++u)
            uploaders.emplace_back([&] {
                for (int i = 0; i < 1000; ++i) {
                    if (!q.try_reserve()) continue;
                    int now = ++in_use;
                    int prev = max_in_use.load();
                    while (now > prev && !max_in_use.compare_exchange_weak(prev, now)) {}
                    if (i % 7 == 0) { --in_use; q.cancel_reservation(); }   // simulate a failed upload
                    else { q.commit(i); ++committed; }
                }
            });
        for (auto& t : uploaders) t.join();
        while (popped < committed) std::this_thread::yield();
        q.close();
        for (auto& t : workers) t.join();
        CHECK(max_in_use <= static_cast<int>(cap) + 2);   // + 2 workers of counting lag
        CHECK(popped == committed);
        std::cout << "5. stress: max places in use = " << max_in_use << " (limit " << cap
                  << " + 2 workers of counting lag), committed = " << committed << ", popped = " << popped << "\n";
    }
    std::cout << (failures ? "SOME TESTS FAILED\n" : "All queue tests passed\n");
    return failures ? 1 : 0;
}
