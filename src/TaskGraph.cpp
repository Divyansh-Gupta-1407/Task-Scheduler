
#include "TaskGraph.h"

#include <queue>
#include <stdexcept>
#include <utility>

TaskGraph::TaskId TaskGraph::addTask(const std::string& name) {
    if (name.empty()) {
        throw std::invalid_argument("Task name cannot be empty");
    }

    const TaskId id = nextId++;

    nodes.emplace(id, Node{name, {}, {}, 0});

    return id;
}

void TaskGraph::addDependency(
    TaskId prerequisite,
    TaskId dependent
) {
    auto source = nodes.find(prerequisite);
    auto target = nodes.find(dependent);

    if (source == nodes.end() || target == nodes.end()) {
        throw std::invalid_argument("Unknown task ID");
    }

    if (prerequisite == dependent) {
        throw std::invalid_argument("A task cannot depend on itself");
    }

    // Prevent duplicate dependency edges.
    for (TaskId existing : source->second.dependents) {
        if (existing == dependent) {
            return;
        }
    }

    source->second.dependents.push_back(dependent);
    target->second.indegree++;
}

std::vector<TaskGraph::TaskId> TaskGraph::topologicalOrder() const {
    std::unordered_map<TaskId, std::size_t> remainingIndegree;

    std::queue<TaskId> ready;

    for (const auto& entry : nodes) {
        remainingIndegree[entry.first] = entry.second.indegree;

        if (entry.second.indegree == 0) {
            ready.push(entry.first);
        }
    }

    std::vector<TaskId> order;
    order.reserve(nodes.size());

    while (!ready.empty()) {
        TaskId current = ready.front();
        ready.pop();

        order.push_back(current);

        for (TaskId dependent : nodes.at(current).dependents) {
            auto& degree = remainingIndegree.at(dependent);

            if (--degree == 0) {
                ready.push(dependent);
            }
        }
    }

    if (order.size() != nodes.size()) {
        throw std::runtime_error(
            "Dependency cycle detected in task graph"
        );
    }

    return order;
}

const std::string& TaskGraph::getName(TaskId id) const {
    auto it = nodes.find(id);

    if (it == nodes.end()) {
        throw std::invalid_argument("Unknown task ID");
    }

    return it->second.name;
}

std::size_t TaskGraph::size() const {
    return nodes.size();
}


void TaskGraph::setTask(TaskId id, TaskFunction function) {
    auto it = nodes.find(id);

    if (it == nodes.end()) {
        throw std::invalid_argument("Unknown task ID");
    }

    if (!function) {
        throw std::invalid_argument("Task function cannot be empty");
    }

    it->second.function = std::move(function);
}

const TaskGraph::TaskFunction& TaskGraph::getTask(TaskId id) const {
    auto it = nodes.find(id);

    if (it == nodes.end()) {
        throw std::invalid_argument("Unknown task ID");
    }

    return it->second.function;
}

const std::vector<TaskGraph::TaskId>&
TaskGraph::getDependents(TaskId id) const {
    auto it = nodes.find(id);

    if (it == nodes.end()) {
        throw std::invalid_argument("Unknown task ID");
    }

    return it->second.dependents;
}

std::size_t TaskGraph::getIndegree(TaskId id) const {
    auto it = nodes.find(id);

    if (it == nodes.end()) {
        throw std::invalid_argument("Unknown task ID");
    }

    return it->second.indegree;
}

std::vector<TaskGraph::TaskId> TaskGraph::getTaskIds() const {
    std::vector<TaskId> ids;
    ids.reserve(nodes.size());

    for (const auto& entry : nodes) {
        ids.push_back(entry.first);
    }

    return ids;
}
