//
// Created by ofacklam on 06/11/2020.
//

#ifndef DA_PROJECT_SYNCHRONIZEDQUEUE_HPP
#define DA_PROJECT_SYNCHRONIZEDQUEUE_HPP

#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>

using namespace std::chrono_literals;

/**
 * Thread-safe queue class
 * Strongly inspired by https://stackoverflow.com/a/16075550
 */
template<class T>
class SynchronizedQueue {
private:
    std::mutex m;
    std::condition_variable cvWriter, cvReader;
    std::queue<T> q;
    unsigned long long capacity;

public:
    SynchronizedQueue() : capacity(0) {}

    explicit SynchronizedQueue(unsigned long long capacity) : capacity(capacity) {}

    void enqueue(T elem) {
        // Wait for queue to have sufficient capacity
        std::unique_lock<std::mutex> lk(m);
        cvWriter.wait(lk, [this] { return capacity == 0 || q.size() < capacity; });

        // Enqueue
        q.push(elem);

        // Notify
        lk.unlock();
        cvReader.notify_one();
    }

    bool dequeue(T *val, unsigned ms) {
        // Wait for queue to be non-empty
        std::unique_lock<std::mutex> lk(m);
        bool success = cvReader.wait_for(lk, ms * 1ms, [this] { return !q.empty(); });
        if (!success)
            return false;

        // Dequeue
        *val = q.front();
        q.pop();

        // Notify
        lk.unlock();
        cvWriter.notify_one();

        return true;
    }
};

#endif //DA_PROJECT_SYNCHRONIZEDQUEUE_HPP
