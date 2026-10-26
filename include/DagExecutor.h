
#ifndef DAG_EXECUTOR_H
#define DAG_EXECUTOR_H

#include "TaskGraph.h"
#include "ThreadPool.h"

#include <string>
#include <unordered_map>

enum class DagTaskStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    BLOCKED
};

struct DagTaskResult {
    DagTaskStatus status = DagTaskStatus::PENDING;
    std::string error;
};

class DagExecutor {
public:
    explicit DagExecutor(ThreadPool& pool);

    std::unordered_map<TaskGraph::TaskId, DagTaskResult>
    execute(const TaskGraph& graph);

private:
    ThreadPool& pool;
};

#endif
