
#include "ThreadPool.h"

#include <algorithm>
#include <cstddef>
#include <stdexcept>
#include <vector>


ThreadPool::ThreadPool(
    std::size_t numThreads,
    std::chrono::milliseconds agingInterval
)
    : agingInterval(agingInterval) {

    if (numThreads == 0) {
        throw std::invalid_argument(
            "Thread pool must have at least one worker"
        );
    }

    if (agingInterval.count() <= 0) {
        throw std::invalid_argument(
            "Aging interval must be positive"
        );
    }

    workers.reserve(numThreads);

    try {
        for (std::size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back(
                &ThreadPool::workerLoop, this
            );
        }
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            stopping = true;
        }

        condition.notify_all();

        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }

        throw;
    }
}

void ThreadPool::workerLoop() {
    while (true) {
        WorkItem item;

        {
            std::unique_lock<std::mutex> lock(queueMutex);

            condition.wait(lock, [this]() {
                return stopping || !tasks.empty();
            });

            if (stopping && tasks.empty()) {
                return;
            }

            const auto now =
                std::chrono::steady_clock::now();

            auto effectivePriority = [this, now](
                const WorkItem& task
            ) -> long long {
                const auto waitedMs =
                    std::chrono::duration_cast<
                        std::chrono::milliseconds
                    >(now - task.enqueuedAt).count();

                const auto agingSteps =
                    waitedMs / agingInterval.count();

                return static_cast<long long>(task.priority)
                       + agingSteps;
            };

              std::vector<WorkItem> queued;
              queued.reserve(tasks.size());
              while (!tasks.empty()) {
                 queued.push_back(tasks.top());
                 tasks.pop();
              }

              auto best = queued.begin();

              for (auto it = queued.begin() + 1;
                  it != queued.end(); ++it) {

                const auto currentScore =
                    effectivePriority(*it);

                const auto bestScore =
                    effectivePriority(*best);

                if (currentScore > bestScore ||
                    (currentScore == bestScore &&
                     it->sequence < best->sequence)) {
                    best = it;
                }
            }

            item = std::move(*best);
            queued.erase(best);

            for (auto& task : queued) {
                tasks.push(std::move(task));
            }
        }

        // Never hold the queue mutex while running user code.
        item.execute();
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        stopping = true;
    }

    condition.notify_all();

    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}
