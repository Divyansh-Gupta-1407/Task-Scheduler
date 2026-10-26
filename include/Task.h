
#ifndef TASK_H
#define TASK_H

#include <chrono>
#include <cstdint>
#include <mutex>
#include <stdexcept>


enum class TaskStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
};


class TaskCancelled : public std::runtime_error {
public:
    TaskCancelled()
        : std::runtime_error("Task was cancelled before execution") {}
};



struct TaskState {
    using Clock = std::chrono::steady_clock;

    const std::uint64_t id;
    const int priority;
    const Clock::time_point submittedAt = Clock::now();

    TaskStatus status = TaskStatus::PENDING;
    Clock::time_point startedAt{};
    Clock::time_point finishedAt{};

    mutable std::mutex mutex;

    TaskState(std::uint64_t taskId, int taskPriority)
        : id(taskId), priority(taskPriority) {}

    TaskStatus getStatus() const {
        std::lock_guard<std::mutex> lock(mutex);
        return status;
    }

    Clock::time_point getStartedAt() const {
        std::lock_guard<std::mutex> lock(mutex);
        return startedAt;
    }

    Clock::time_point getFinishedAt() const {
        std::lock_guard<std::mutex> lock(mutex);
        return finishedAt;
    }

    double queueWaitMs() const {
        std::lock_guard<std::mutex> lock(mutex);

        if (startedAt == Clock::time_point{}) {
            return -1.0;
        }

        return std::chrono::duration<double, std::milli>(
            startedAt - submittedAt
        ).count();
    }

    double executionMs() const {
        std::lock_guard<std::mutex> lock(mutex);

        if (startedAt == Clock::time_point{} ||
            finishedAt == Clock::time_point{}) {
            return -1.0;
        }

        return std::chrono::duration<double, std::milli>(
            finishedAt - startedAt
        ).count();
    }

    
    bool cancelPending() {
        std::lock_guard<std::mutex> lock(mutex);

        if (status != TaskStatus::PENDING) {
            return false;
        }

        status = TaskStatus::CANCELLED;
        finishedAt = Clock::now();

        return true;
    }
};

#endif
