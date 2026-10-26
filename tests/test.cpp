
#include "ThreadPool.h"
#include "TaskGraph.h"
#include "DagExecutor.h"

#include <iomanip>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

struct ResultRow {
    std::uint64_t id;
    int priority;
    std::string status;
    std::string result;
    double waitMs;
    double executionMs;
};

std::string statusToString(TaskStatus status) {
    switch (status) {
        case TaskStatus::PENDING:   return "PENDING";
        case TaskStatus::RUNNING:   return "RUNNING";
        case TaskStatus::COMPLETED: return "COMPLETED";
        case TaskStatus::FAILED:    return "FAILED";
        case TaskStatus::CANCELLED: return "CANCELLED";
    }

    return "UNKNOWN";
}

int main() {
    std::cout << std::fixed << std::setprecision(2);
    bool allTestsPassed = true;

    // ------------------------------------------
    // TEST 1: Priority scheduling and task metrics
    // ------------------------------------------

    std::cout << "TEST 1: Priority Scheduling\n\n";

    ThreadPool pool(1);

    std::promise<void> releaseWorker;
    auto gate = releaseWorker.get_future().share();

    std::promise<void> workerStarted;
    auto started = workerStarted.get_future();

    auto blocker = pool.submit(0, [&]() {
        workerStarted.set_value();
        gate.wait();
    });

    started.wait();

    std::vector<int> executionOrder;
    std::mutex orderMutex;
    std::vector<TaskHandle<int>> jobs;

    auto addTask = [&](int priority, int result) {
        return pool.submit(priority, [&, priority, result]() {
            {
                std::lock_guard<std::mutex> lock(orderMutex);
                executionOrder.push_back(priority);
            }

            return result;
        });
    };

    // Deliberately submit tasks out of priority order.
    jobs.push_back(addTask(2, 20));
    jobs.push_back(addTask(10, 100));
    jobs.push_back(addTask(1, 10));
    jobs.push_back(addTask(8, 80));

    // Include a task that throws an exception.
    jobs.push_back(pool.submit(5, [&]() -> int {
        {
            std::lock_guard<std::mutex> lock(orderMutex);
            executionOrder.push_back(5);
        }

        throw std::runtime_error("Simulated task failure");
    }));

    releaseWorker.set_value();
    blocker.get();

    std::vector<ResultRow> report;
    report.reserve(jobs.size());

    for (auto& job : jobs) {
        std::string result;

        try {
            result = std::to_string(job.get());
        } catch (const std::exception& e) {
            result = std::string("Error: ") + e.what();
        }

        report.push_back({
            job.id(),
            job.priority(),
            statusToString(job.status()),
            result,
            job.queueWaitMs(),
            job.executionMs()
        });
    }

    std::cout << "Execution priority order: ";

    for (size_t i = 0; i < executionOrder.size(); ++i) {
        if (i > 0) std::cout << " -> ";
        std::cout << executionOrder[i];
    }

    std::cout << "\n\n";

    std::cout << std::left
              << std::setw(9)  << "ID"
              << std::setw(10) << "Priority"
              << std::setw(12) << "Status"
              << std::setw(30) << "Result"
              << std::setw(13) << "Wait (ms)"
              << std::setw(13) << "Run (ms)"
              << '\n';

    std::cout << std::string(87, '-') << '\n';

    for (const auto& row : report) {
        std::cout << std::left
                  << std::setw(9)  << row.id
                  << std::setw(10) << row.priority
                  << std::setw(12) << row.status
                  << std::setw(30) << row.result
                  << std::setw(13) << row.waitMs
                  << std::setw(13) << row.executionMs
                  << '\n';
    }

    const std::vector<int> expectedOrder = {10, 8, 5, 2, 1};
    const bool priorityPassed = executionOrder == expectedOrder;
    const bool failureStatusPassed =
        jobs.back().status() == TaskStatus::FAILED;

    std::cout << "\nPriority ordering: "
              << (priorityPassed ? "PASS" : "FAIL")
              << '\n';
    std::cout << "Exception status: "
              << (failureStatusPassed ? "PASS" : "FAIL")
              << '\n';
    allTestsPassed = allTestsPassed && priorityPassed && failureStatusPassed;

    // ------------------------------------------
    // TEST 2: Concurrent task submissions
    // ------------------------------------------

    std::cout << "\nTEST 2: Concurrent Submissions\n\n";

    ThreadPool concurrentPool(4);

    constexpr int producerCount = 4;
    constexpr int tasksPerProducer = 25;

    std::vector<TaskHandle<int>> concurrentHandles;
    std::mutex handlesMutex;
    std::vector<std::thread> producers;

    for (int p = 0; p < producerCount; ++p) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < tasksPerProducer; ++i) {
                int value = p * tasksPerProducer + i;

                auto handle = concurrentPool.submit(1, [value]() {
                    return value * 2;
                });

                std::lock_guard<std::mutex> lock(handlesMutex);
                concurrentHandles.push_back(std::move(handle));
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    const int expectedTasks = producerCount * tasksPerProducer;
    int successfulResults = 0;
    bool resultsCorrect = true;
    std::vector<bool> seenResults(
        producerCount * tasksPerProducer,
        false
    );

    for (auto& handle : concurrentHandles) {
        try {
            int result = handle.get();
            ++successfulResults;

            if (result % 2 != 0 ||
                result / 2 < 0 ||
                result / 2 >= expectedTasks ||
                seenResults[static_cast<std::size_t>(result / 2)]) {
                resultsCorrect = false;
            } else {
                seenResults[static_cast<std::size_t>(result / 2)] = true;
            }
        } catch (...) {
            resultsCorrect = false;
        }
    }

    std::cout << "Tasks submitted: " << concurrentHandles.size() << '\n';
    std::cout << "Tasks completed: " << successfulResults << '\n';
    std::cout << "Results valid: "
              << (resultsCorrect && successfulResults == expectedTasks
                      ? "PASS" : "FAIL")
              << '\n';
    const bool concurrentPassed =
        resultsCorrect && successfulResults == expectedTasks;
    allTestsPassed = allTestsPassed && concurrentPassed;

    // ------------------------------------------
    // TEST 3: Priority aging
    // ------------------------------------------

    std::cout << "\nTEST 3: Priority Aging\n\n";

    ThreadPool agingPool(
        1,
        std::chrono::milliseconds(20)
    );

    std::promise<void> releaseAgingWorker;
    auto agingGate = releaseAgingWorker.get_future().share();

    std::promise<void> agingWorkerStarted;
    auto agingStarted = agingWorkerStarted.get_future();

    auto agingBlocker = agingPool.submit(0, [&]() {
        agingWorkerStarted.set_value();
        agingGate.wait();
    });

    agingStarted.wait();

    std::vector<int> agingOrder;

    // This task waits in the queue and gradually gains priority.
    auto oldTask = agingPool.submit(1, [&]() {
        agingOrder.push_back(1);
        return 1;
    });

    // Simulate a task waiting long enough to gain priority.
    std::this_thread::sleep_for(
        std::chrono::milliseconds(220)
    );

    // A newer task arrives with a higher base priority.
    auto newTask = agingPool.submit(10, [&]() {
        agingOrder.push_back(10);
        return 10;
    });

    releaseAgingWorker.set_value();

    agingBlocker.get();

    int oldResult = oldTask.get();
    int newResult = newTask.get();

    bool agingPassed =
        oldResult == 1 &&
        newResult == 10 &&
        agingOrder == std::vector<int>{1, 10};
    allTestsPassed = allTestsPassed && agingPassed;

    std::cout << "Execution order: ";

    for (size_t i = 0; i < agingOrder.size(); ++i) {
        if (i > 0) std::cout << " -> ";
        std::cout << agingOrder[i];
    }

    std::cout << "\nPriority aging: "
              << (agingPassed ? "PASS" : "FAIL")
              << '\n';

    // ------------------------------------------
    // TEST 4: Pending task cancellation
    // ------------------------------------------

    std::cout << "\nTEST 4: Task Cancellation\n\n";

    ThreadPool cancelPool(1);

    std::promise<void> releaseCancelWorker;
    auto cancelGate = releaseCancelWorker.get_future().share();

    std::promise<void> cancelWorkerStarted;
    auto cancelStarted = cancelWorkerStarted.get_future();

    auto cancelBlocker = cancelPool.submit(0, [&]() {
        cancelWorkerStarted.set_value();
        cancelGate.wait();
    });

    cancelStarted.wait();

    bool taskExecuted = false;

    auto cancelledTask = cancelPool.submit(10, [&]() {
        taskExecuted = true;
        return 42;
    });

    const bool pendingWaitTimedOut =
        cancelledTask.waitFor(std::chrono::milliseconds(0)) ==
        std::future_status::timeout;
    const bool blockerWaitTimedOut =
        cancelBlocker.waitFor(std::chrono::milliseconds(0)) ==
        std::future_status::timeout;

    bool cancelAccepted = cancelledTask.cancel();

    releaseCancelWorker.set_value();

    const bool blockerWaitBecameReady =
        cancelBlocker.waitFor(std::chrono::seconds(1)) ==
        std::future_status::ready;
    cancelBlocker.get();

    const bool cancelledWaitBecameReady =
        cancelledTask.waitFor(std::chrono::seconds(1)) ==
        std::future_status::ready;

    bool cancellationReported = false;

    try {
        cancelledTask.get();
    } catch (const TaskCancelled&) {
        cancellationReported = true;
    }

    bool cancellationPassed =
        cancelAccepted &&
        cancellationReported &&
        !taskExecuted &&
        cancelledTask.status() == TaskStatus::CANCELLED &&
        pendingWaitTimedOut &&
        blockerWaitTimedOut &&
        blockerWaitBecameReady &&
        cancelledWaitBecameReady;

    std::cout << "Cancellation accepted: "
              << (cancelAccepted ? "YES" : "NO") << '\n';

    std::cout << "Task executed: "
              << (taskExecuted ? "YES" : "NO") << '\n';

    std::cout << "Final status: "
              << statusToString(cancelledTask.status()) << '\n';
    std::cout << "Timed waits: "
              << (pendingWaitTimedOut && blockerWaitTimedOut &&
                          blockerWaitBecameReady && cancelledWaitBecameReady
                      ? "PASS" : "FAIL")
              << '\n';

    std::cout << "Cancellation test: "
              << (cancellationPassed ? "PASS" : "FAIL") << '\n';
    allTestsPassed = allTestsPassed && cancellationPassed;

    // ------------------------------------------
    // TEST 5: DAG dependencies and cycle detection
    // ------------------------------------------

    std::cout << "\nTEST 5: Task Dependency Graph\n\n";

    TaskGraph graph;

    auto graphCollect = graph.addTask("Collect data");
    auto graphClean = graph.addTask("Clean data");
    auto graphReport = graph.addTask("Generate report");
    auto graphNotify = graph.addTask("Send notification");

    graph.addDependency(graphCollect, graphClean);
    graph.addDependency(graphClean, graphReport);

    // Notification is independent of the data pipeline.

    auto order = graph.topologicalOrder();

    auto position = [&](TaskGraph::TaskId id) {
        for (size_t i = 0; i < order.size(); ++i) {
            if (order[i] == id) {
                return i;
            }
        }

        return order.size();
    };

    bool validOrder =
        position(graphCollect) < position(graphClean) &&
        position(graphClean) < position(graphReport) &&
        position(graphNotify) < order.size() &&
        order.size() == 4;

    std::cout << "Valid topological order: "
              << (validOrder ? "PASS" : "FAIL") << '\n';

    TaskGraph cyclicGraph;

    auto a = cyclicGraph.addTask("A");
    auto b = cyclicGraph.addTask("B");
    auto c = cyclicGraph.addTask("C");

    cyclicGraph.addDependency(a, b);
    cyclicGraph.addDependency(b, c);
    cyclicGraph.addDependency(c, a);

    bool cycleDetected = false;

    try {
        cyclicGraph.topologicalOrder();
    } catch (const std::runtime_error&) {
        cycleDetected = true;
    }

    std::cout << "Cycle detection: "
              << (cycleDetected ? "PASS" : "FAIL") << '\n';

    allTestsPassed = allTestsPassed && validOrder && cycleDetected;

    // ------------------------------------------
    // TEST 6: DAG execution with ThreadPool
    // ------------------------------------------

    std::cout << "\nTEST 6: DAG Execution\n\n";

    ThreadPool dagPool(4);
    DagExecutor executor(dagPool);

    TaskGraph workflow;

    auto workflowCollect = workflow.addTask("Collect data");
    auto workflowClean = workflow.addTask("Clean data");
    auto workflowReport = workflow.addTask("Generate report");
    auto workflowNotify = workflow.addTask("Send notification");

    std::vector<int> workflowOrder;
    std::mutex workflowMutex;

    auto record = [&](int value) {
        std::lock_guard<std::mutex> lock(workflowMutex);
        workflowOrder.push_back(value);
    };

    workflow.setTask(workflowCollect, [&]() {
        record(1);
    });

    workflow.setTask(workflowClean, [&]() {
        record(2);
    });

    workflow.setTask(workflowReport, [&]() {
        record(3);
    });

    workflow.setTask(workflowNotify, [&]() {
        record(4);
    });

    workflow.addDependency(workflowCollect, workflowClean);
    workflow.addDependency(workflowClean, workflowReport);

    auto dagResults = executor.execute(workflow);

    auto positionOf = [&](int value) {
        for (size_t i = 0; i < workflowOrder.size(); ++i) {
            if (workflowOrder[i] == value) {
                return i;
            }
        }

        return workflowOrder.size();
    };

    bool dependenciesRespected =
        positionOf(1) < positionOf(2) &&
        positionOf(2) < positionOf(3) &&
        workflowOrder.size() == 4;

    bool allCompleted = true;

    for (const auto& entry : dagResults) {
        if (entry.second.status != DagTaskStatus::COMPLETED) {
            allCompleted = false;
        }
    }

    std::cout << "Workflow execution order: ";

    for (size_t i = 0; i < workflowOrder.size(); ++i) {
        if (i > 0) std::cout << " -> ";
        std::cout << workflowOrder[i];
    }

    std::cout << '\n';

    std::cout << "Dependencies respected: "
              << (dependenciesRespected ? "PASS" : "FAIL") << '\n';

    std::cout << "All tasks completed: "
              << (allCompleted ? "PASS" : "FAIL") << '\n';

    allTestsPassed = allTestsPassed &&
        dependenciesRespected && allCompleted;

    std::cout << "\nAll tests finished.\n";

    
    // ------------------------------------------
    // TEST 7: DAG failure propagation
    // ------------------------------------------

    std::cout << "\nTEST 7: DAG Failure Propagation\n\n";

    ThreadPool failurePool(2);
    DagExecutor failureExecutor(failurePool);

    TaskGraph failureGraph;

    auto source = failureGraph.addTask("Source task");
    auto dependent = failureGraph.addTask("Dependent task");
    auto independent = failureGraph.addTask("Independent task");

    bool dependentExecuted = false;
    bool independentExecuted = false;

    failureGraph.setTask(source, []() {
        throw std::runtime_error("Source failed");
    });

    failureGraph.setTask(dependent, [&]() {
        dependentExecuted = true;
    });

    failureGraph.setTask(independent, [&]() {
        independentExecuted = true;
    });

    failureGraph.addDependency(source, dependent);

    auto failureResults = failureExecutor.execute(failureGraph);

    bool failureCorrect =
        failureResults.at(source).status == DagTaskStatus::FAILED &&
        failureResults.at(dependent).status == DagTaskStatus::BLOCKED &&
        failureResults.at(independent).status == DagTaskStatus::COMPLETED &&
        !dependentExecuted &&
        independentExecuted;

    std::cout << "Source status: FAILED\n";
    std::cout << "Dependent status: BLOCKED\n";
    std::cout << "Independent task: "
              << (independentExecuted ? "COMPLETED" : "NOT COMPLETED")
              << '\n';

    std::cout << "Failure propagation: "
              << (failureCorrect ? "PASS" : "FAIL") << '\n';


    return allTestsPassed ? 0 : 1;
}
