
#include "DagExecutor.h"

#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

DagExecutor::DagExecutor(ThreadPool& pool) : pool(pool) {}

std::unordered_map<TaskGraph::TaskId, DagTaskResult>
DagExecutor::execute(const TaskGraph& graph) {
    using TaskId = TaskGraph::TaskId;

    // Validate the complete graph before running any task.
    graph.topologicalOrder();

    const auto ids = graph.getTaskIds();

    std::unordered_map<TaskId, DagTaskResult> results;
    std::unordered_map<TaskId, std::size_t> remaining;
    std::unordered_map<TaskId, TaskHandle<void>> running;

    std::vector<TaskId> ready;

    for (TaskId id : ids) {
        if (!graph.getTask(id)) {
            throw std::invalid_argument(
                "Every DAG task must have a function: " +
                graph.getName(id)
            );
        }

        results.emplace(id, DagTaskResult{});
        remaining[id] = graph.getIndegree(id);

        if (remaining[id] == 0) {
            ready.push_back(id);
        }
    }

    std::size_t finished = 0;

    auto blockDescendants = [&](TaskId failedId) {
        std::vector<TaskId> stack = graph.getDependents(failedId);
        
        while (!stack.empty()) {
            TaskId current = stack.back();
            stack.pop_back();

            auto& result = results.at(current);

            if (result.status != DagTaskStatus::PENDING) {
                continue;
            }

            result.status = DagTaskStatus::BLOCKED;
            ++finished;

            const auto& children = graph.getDependents(current);

            for (TaskId child : children) {
                stack.push_back(child);
            }
        }
    };

    while (finished < ids.size()) {
        // Submit every currently ready task.
        while (!ready.empty()) {
            TaskId id = ready.back();
            ready.pop_back();

            if (results.at(id).status != DagTaskStatus::PENDING) {
                continue;
            }

            results.at(id).status = DagTaskStatus::RUNNING;

            try {
                auto handle = pool.submit(0, graph.getTask(id));
                running.emplace(id, std::move(handle));
            } catch (const std::exception& e) {
                results.at(id).status = DagTaskStatus::FAILED;
                results.at(id).error = e.what();

                ++finished;
                blockDescendants(id);
            }
        }

        // Poll running tasks for completion.
        bool observedCompletion = false;

        for (auto it = running.begin(); it != running.end();) {
            if (it->second.waitFor(std::chrono::milliseconds(0)) !=
                std::future_status::ready) {
                ++it;
                continue;
            }

            const TaskId id = it->first;
            bool success = true;

            try {
                it->second.get();
            } catch (const std::exception& e) {
                success = false;
                results.at(id).error = e.what();
            } catch (...) {
                success = false;
                results.at(id).error = "Unknown task exception";
            }

            results.at(id).status = success
                ? DagTaskStatus::COMPLETED
                : DagTaskStatus::FAILED;

            ++finished;
            observedCompletion = true;

            if (success) {
                for (TaskId child : graph.getDependents(id)) {
                    auto& count = remaining.at(child);

                    if (count > 0) {
                        --count;
                    }

                    if (count == 0 &&
                        results.at(child).status ==
                            DagTaskStatus::PENDING) {
                        ready.push_back(child);
                    }
                }
            } else {
                blockDescendants(id);
            }

            it = running.erase(it);
        }

        // If tasks remain, none are running or ready, and no progress
        // was made, the workflow cannot proceed.
        if (running.empty() && ready.empty() && finished < ids.size()) {
            throw std::logic_error(
                "DAG execution stalled: unresolved task states"
            );
        }

        // Avoid busy-spinning when all active tasks are still running.
        if (!observedCompletion && ready.empty() && !running.empty()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(1)
            );
        }
    }

    return results;
}
