
#ifndef TASK_GRAPH_H
#define TASK_GRAPH_H

#include <cstddef>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

class TaskGraph {
public:
    using TaskId = int;
    using TaskFunction = std::function<void()>;

    TaskId addTask(const std::string& name);

    void setTask(TaskId id, TaskFunction function);

    void addDependency(TaskId prerequisite, TaskId dependent);

    std::vector<TaskId> topologicalOrder() const;

    const std::string& getName(TaskId id) const;
    const TaskFunction& getTask(TaskId id) const;

    const std::vector<TaskId>& getDependents(TaskId id) const;
    std::size_t getIndegree(TaskId id) const;

    std::vector<TaskId> getTaskIds() const;
    std::size_t size() const;

private:
    struct Node {
        std::string name;
        TaskFunction function;
        std::vector<TaskId> dependents;
        std::size_t indegree = 0;
    };

    std::unordered_map<TaskId, Node> nodes;
    TaskId nextId = 1;
};

#endif
