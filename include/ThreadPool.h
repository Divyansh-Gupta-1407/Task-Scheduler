
#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include "Task.h"

#include <cstddef>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

template <typename T>
class TaskHandle {
public:
    TaskHandle(std::future<T> result, std::shared_ptr<TaskState> state)
        : result(std::move(result)), state(std::move(state)) {}

    TaskHandle(TaskHandle&&) = default;
    TaskHandle& operator=(TaskHandle&&) = default;

    TaskHandle(const TaskHandle&) = delete;
    TaskHandle& operator=(const TaskHandle&) = delete;

    T get() {
        return result.get();
    }

    void wait() const {
        result.wait();
    }

    std::future_status waitFor(std::chrono::milliseconds timeout) const {
        return result.wait_for(timeout);
    }

    std::uint64_t id() const {
        return state->id;
    }

    int priority() const {
        return state->priority;
    }

    TaskStatus status() const {
        return state->getStatus();
    }

    double queueWaitMs() const {
        return state->queueWaitMs();
    }

    double executionMs() const {
        return state->executionMs();
    }

    bool cancel() {
        return state->cancelPending();
    }



private:
    std::future<T> result;
    std::shared_ptr<TaskState> state;
};

template <>
class TaskHandle<void> {
public:
    TaskHandle(std::future<void> result, std::shared_ptr<TaskState> state)
        : result(std::move(result)), state(std::move(state)) {}

    TaskHandle(TaskHandle&&) = default;
    TaskHandle& operator=(TaskHandle&&) = default;

    TaskHandle(const TaskHandle&) = delete;
    TaskHandle& operator=(const TaskHandle&) = delete;

    void get() {
        result.get();
    }

    void wait() const {
        result.wait();
    }

    std::future_status waitFor(std::chrono::milliseconds timeout) const {
        return result.wait_for(timeout);
    }

    std::uint64_t id() const {
        return state->id;
    }

    int priority() const {
        return state->priority;
    }

    TaskStatus status() const {
        return state->getStatus();
    }

    double queueWaitMs() const {
        return state->queueWaitMs();
    }

    double executionMs() const {
        return state->executionMs();
    }
    
    bool cancel() {
        return state->cancelPending();
    }



private:
    std::future<void> result;
    std::shared_ptr<TaskState> state;
};

class ThreadPool {
public:
    explicit ThreadPool(
        std::size_t numThreads,
        std::chrono::milliseconds agingInterval =
            std::chrono::milliseconds(1000)
    );
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    template <typename F, typename... Args>
    auto submit(int priority, F&& f, Args&&... args)
        -> TaskHandle<std::invoke_result_t<F, Args...>> {

        using ReturnType = std::invoke_result_t<F, Args...>;

        auto bound = std::bind(
            std::forward<F>(f),
            std::forward<Args>(args)...
        );

        std::unique_lock<std::mutex> lock(queueMutex);

        if (stopping) {
            throw std::runtime_error("Cannot submit to stopped pool");
        }

        auto state = std::make_shared<TaskState>(
            nextTaskId++, priority
        );

        auto task =
            std::make_shared<std::packaged_task<ReturnType()>>(
                [state, fn = std::move(bound)]() mutable -> ReturnType {
                    {
                        std::lock_guard<std::mutex> guard(state->mutex);

                        if (state->status == TaskStatus::CANCELLED) {
                            throw TaskCancelled();
                        }

                        state->status = TaskStatus::RUNNING;
                        state->startedAt = TaskState::Clock::now();
                    }

                    try {
                        if constexpr (std::is_void_v<ReturnType>) {
                            fn();

                            std::lock_guard<std::mutex> guard(state->mutex);
                            state->status = TaskStatus::COMPLETED;
                            state->finishedAt = TaskState::Clock::now();
                            return;
                        } else {
                            ReturnType value = fn();

                            std::lock_guard<std::mutex> guard(state->mutex);
                            state->status = TaskStatus::COMPLETED;
                            state->finishedAt = TaskState::Clock::now();
                            return value;
                        }
                    } catch (...) {
                        std::lock_guard<std::mutex> guard(state->mutex);
                        state->status = TaskStatus::FAILED;
                        state->finishedAt = TaskState::Clock::now();
                        throw;
                    }
                }
            );

        auto result = task->get_future();

        tasks.push(WorkItem{
            priority,
            nextSequence++,
            std::chrono::steady_clock::now(),
            [task]() { (*task)(); }
        });

        lock.unlock();
        condition.notify_one();

        return TaskHandle<ReturnType>(std::move(result), state);
    }

private:
    
    struct WorkItem {
        int priority;
        std::uint64_t sequence;
        std::chrono::steady_clock::time_point enqueuedAt;
        std::function<void()> execute;
    };

    void workerLoop();

    std::vector<std::thread> workers;
    struct CompareTasks {
        bool operator()(const WorkItem& a, const WorkItem& b) const {
            if (a.priority != b.priority) {
                return a.priority < b.priority;
            }

            return a.sequence > b.sequence;
        }
    };

    std::priority_queue<
        WorkItem,
        std::vector<WorkItem>,
        CompareTasks
    > tasks;

    std::chrono::milliseconds agingInterval;
    std::mutex queueMutex;
    std::condition_variable condition;
    bool stopping = false;

    std::uint64_t nextTaskId = 1;
    std::uint64_t nextSequence = 0;
};

#endif
