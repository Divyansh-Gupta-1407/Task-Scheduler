# Multithreaded Task Scheduler in C++

A C++17 task scheduling system that executes tasks concurrently using a worker pool, schedules work according to priority, tracks task execution, and supports dependency-aware execution through a Directed Acyclic Graph (DAG).

**Project status:** Core functionality implemented; reliability improvements, automated testing, and benchmarking are next.

## Table of Contents

- Overview
- Features
- Architecture
- Project Structure
- Scheduling and Execution
- DAG-Based Execution
- Task Lifecycle and Error Handling
- Tech Stack
- Getting Started
- Testing
- Current Limitations
- Roadmap
- Key Concepts
- Author

## 1. Overview

Applications often need to execute multiple tasks efficiently without creating a new operating-system thread for every task. A task scheduler manages when work is queued, which tasks execute first, how workers coordinate, and how failures are reported.

This project implements a multithreaded task scheduler in C++ using a fixed-size worker pool. It extends basic concurrent execution with priority scheduling, priority aging, cancellation of pending tasks, execution metrics, and a DAG executor for tasks that depend on one another.

The goal is to explore the engineering concepts behind concurrent systems, including synchronization, task lifecycle management, scheduling policies, dependency resolution, and failure propagation.

## 2. Features

### Implemented

- **Worker thread pool:** Reuses a fixed set of worker threads to execute submitted tasks.
- **Concurrent task submission:** Allows tasks to be submitted to a shared, synchronized work queue.
- **Priority-based scheduling:** Selects queued tasks based on priority, with higher integer values representing higher priority.
- **Priority aging:** Increases a task's effective scheduling priority as it waits, helping reduce starvation.
- **Task lifecycle tracking:** Tracks `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, and `CANCELLED` states.
- **Future-based results:** Uses `std::future` to retrieve return values and exceptions from submitted tasks.
- **Pending-task cancellation:** Allows a task to be cancelled before execution begins.
- **Execution metrics:** Records queue wait time and execution duration for tasks that have started.
- **DAG dependency management:** Models prerequisite relationships between tasks.
- **Cycle detection:** Uses topological sorting to detect invalid cyclic dependencies.
- **Dependency-aware execution:** Runs tasks after their prerequisites have completed successfully.
- **Failure propagation:** Marks dependent tasks as blocked when a prerequisite fails.
- **Graceful shutdown:** Stops accepting new work and drains queued tasks before worker threads exit.

### Planned

- Automated test integration with CTest.
- More extensive concurrency and stress testing.
- Performance benchmarks against sequential execution and alternative configurations.
- Improved DAG completion notification, replacing periodic polling.
- Stronger shutdown and cancellation tests.
- ThreadSanitizer or other suitable race-detection tooling where supported.
- More comprehensive API documentation and usage examples.

## 3. Architecture

The project consists of four main components.

### ThreadPool

Manages worker threads and the shared task queue.

Responsibilities:

- Create and manage worker threads.
- Accept submitted tasks.
- Select the next queued task using priority and aging.
- Synchronize access to shared scheduling state.
- Shut down gracefully and join workers.

### TaskState

Stores metadata for each submitted task.

Responsibilities:

- Maintain the task ID and original priority.
- Track the task's lifecycle state.
- Record submission, start, and finish timestamps.
- Calculate queue wait time and execution duration.
- Coordinate cancellation of pending tasks.

### TaskGraph

Represents tasks and their dependency relationships.

Responsibilities:

- Register named tasks and their functions.
- Add prerequisite-to-dependent edges.
- Track dependency counts.
- Return a topological ordering.
- Detect cycles before execution.

### DagExecutor

Coordinates execution of a task graph using the thread pool.

Responsibilities:

- Validate the graph before execution.
- Schedule tasks whose prerequisites have completed successfully.
- Collect task results and exceptions.
- Release dependent tasks when prerequisites succeed.
- Block descendants of failed tasks.

### High-level architecture

```text
                    Application
                         |
                         v
                    Task Submission
                         |
                         v
                    +------------+
                    | ThreadPool |
                    +------------+
                         |
                         v
                Shared Work Queue
                (Priority + Aging)
                         |
             +-----------+-----------+
             |           |           |
             v           v           v
          Worker 1    Worker 2    Worker N
             |           |           |
             +-----------+-----------+
                         |
                         v
                    TaskState
             (Status and Timing Data)


                    TaskGraph
                         |
                         v
                    DagExecutor
                         |
                         v
              Dependency Resolution
                         |
                         v
                    ThreadPool
```

The thread pool is responsible for concurrent execution. The DAG executor is responsible for determining which tasks are eligible to execute.

## 4. Project Structure

```text
Schedule/
├── include/
│   ├── Task.h
│   ├── ThreadPool.h
│   ├── TaskGraph.h
│   └── DagExecutor.h
├── src/
│   ├── ThreadPool.cpp
│   ├── TaskGraph.cpp
│   └── DagExecutor.cpp
├── tests/
│   └── test.cpp
├── CMakeLists.txt
└── README.md
```

| File              | Purpose                                                        |
| ----------------- | -------------------------------------------------------------- |
| `Task.h`          | Task states, metadata, timing, and cancellation logic          |
| `ThreadPool.h`    | Thread pool interface, task submission, and task handles       |
| `ThreadPool.cpp`  | Worker management, synchronized queue, and scheduling policy   |
| `TaskGraph.h`     | Task graph interface and dependency representation             |
| `TaskGraph.cpp`   | Graph construction, dependency management, and cycle detection |
| `DagExecutor.h`   | DAG execution interface and result types                       |
| `DagExecutor.cpp` | Dependency-aware scheduling and failure propagation            |
| `tests/test.cpp`  | Functional test cases and execution demonstrations             |
| `CMakeLists.txt`  | Build configuration and source dependencies                    |

## 5. Scheduling and Execution

### Worker pool

Instead of creating a thread for each task, the scheduler maintains a fixed number of worker threads.

Workers wait for queued work using `std::condition_variable`. Once a worker selects a task, it removes that task while holding the queue mutex, then executes the task after releasing the mutex.

This prevents user-defined task code from holding the scheduler's queue lock while it runs.

### Priority scheduling

Each queued task has an integer priority. A larger value represents a higher priority.

When a worker becomes available, the scheduler scans the pending work queue and chooses the task with the highest effective priority. Tasks with equal effective priority are ordered by submission sequence.

### Priority aging

Strict priority scheduling can starve low-priority tasks if higher-priority work keeps arriving.

To reduce this risk, the scheduler increases a task's effective priority according to how long it has waited.

Conceptually:

```text
effective_priority =
    base_priority + floor(wait_time / aging_interval)
```

For example, with an aging interval of one second, a task with base priority `2` that waits for three complete seconds receives an effective priority of `5`.

Aging helps waiting tasks compete for execution, although it does not guarantee fairness under every possible workload.

### Task handles and futures

Submitting a task returns a task handle containing a future and shared task metadata.

The future allows callers to:

- Wait for task completion.
- Retrieve the task's return value.
- Observe exceptions raised by the task.
- Check whether the future is ready.

The handle also exposes the task ID, priority, lifecycle status, timing metrics, and pending-task cancellation operation.

### Cancellation

Cancellation is supported while a task remains pending.

The scheduler changes its state to `CANCELLED`. The queued work item may remain in the internal queue until selected, at which point its callable is skipped and cancellation is reported through the future.

Cancellation is not preemptive: once a task transitions to `RUNNING`, the current implementation does not forcibly stop its execution.

## 6. DAG-Based Task Execution

Some workloads contain dependencies. For example, compiling a source file must happen before linking the resulting object file.

The scheduler represents such relationships using a directed graph.

An edge:

```text
A -> B
```

means task `B` depends on task `A`.

### Dependency resolution

The executor maintains the number of unfinished prerequisites for each task.

1. Validate the graph and detect cycles.
2. Identify tasks with zero prerequisites.
3. Submit eligible tasks to the thread pool.
4. Wait for task completion and collect its result.
5. If the task succeeds, decrement the prerequisite counts of its dependents.
6. Schedule dependents whose prerequisite counts reach zero.
7. If the task fails, mark its pending descendants as `BLOCKED`.

Independent tasks can execute concurrently, subject to the available worker count.

### Example

```text
          A
         / \
        v   v
        B   C
         \ /
          v
          D
```

- `A` must complete before `B` and `C` can run.
- `B` and `C` can execute concurrently.
- `D` runs only after both `B` and `C` succeed.
- If `B` fails, `D` is blocked because not all its prerequisites can succeed.

The graph is validated before execution using Kahn's topological sorting algorithm. If the graph contains a cycle, execution is rejected.

## 7. Task Lifecycle and Error Handling

### Thread pool task states

```text
PENDING -> RUNNING -> COMPLETED
                    -> FAILED

PENDING -> CANCELLED
```

A task that fails stores its exception in the future and is marked `FAILED`. The caller observes the exception when retrieving the result.

The DAG executor maintains its own result states:

- `PENDING`
- `RUNNING`
- `COMPLETED`
- `FAILED`
- `BLOCKED`

These distinguish ordinary task failure from a task that cannot run because a prerequisite failed.

The thread pool also rejects submissions after shutdown has begun.

## 8. Tech Stack

- **Language:** C++17
- **Build system:** CMake
- **Concurrency:** `std::thread`, `std::mutex`, `std::condition_variable`
- **Task results:** `std::future`, `std::packaged_task`
- **Data structures:** `std::vector`, `std::unordered_map`, `std::function`
- **Graph algorithm:** Kahn's algorithm for topological sorting
- **Timing:** `std::chrono::steady_clock`

No external runtime dependency is currently required beyond a compatible C++ compiler, CMake, and the standard threading library.

## 9. Getting Started

### Prerequisites

Install:

- A C++17-compatible compiler.
- CMake 3.16 or newer.
- A supported build tool, such as Visual Studio Build Tools on Windows or GCC/Clang with Make or Ninja.

### Build on Windows using PowerShell

Run these commands from the project root.

**1. Configure the build:**

```powershell
cmake -S . -B build
```

**2. Compile the project:**

```powershell
cmake --build build
```

**3. Run the executable:**

For a Visual Studio multi-configuration generator:

```powershell
.\build\Debug\thread_pool_demo.exe
```

For a single-configuration generator that places the executable directly in `build`:

```powershell
.\build\thread_pool_demo.exe
```

If the executable is not found, inspect the output of `cmake --build build` to determine its generated path.

### Example usage

The following illustrates the intended task-submission interface; it is a conceptual usage example rather than a guarantee that this exact snippet exists in the current test file.

```cpp
ThreadPool pool(4);

auto task = pool.submit(5, [] {
    return 42;
});

int result = task.get();
```

The task is submitted at priority `5`, executed by an available worker, and its return value is retrieved through the task handle.

## 10. Testing

The current test program exercises the major functional components of the scheduler.

| Test area              | What it verifies                                                     |
| ---------------------- | -------------------------------------------------------------------- |
| Priority scheduling    | Higher-priority work is selected ahead of lower-priority queued work |
| Concurrent submissions | Multiple submissions are handled correctly                           |
| Priority aging         | A waiting task's effective priority increases over time              |
| Task cancellation      | A pending task can be cancelled without running its function         |
| Task graph validation  | Dependency relationships and cycle detection                         |
| DAG execution          | Tasks execute after their prerequisites succeed                      |
| Failure propagation    | Failed tasks cause pending dependent tasks to become blocked         |

Observed test output has confirmed the priority scheduling and concurrent-submission tests, along with DAG failure propagation. The complete suite should be rerun after any subsequent changes.

### Recommended test command

```powershell
cmake --build build
.\build\Debug\thread_pool_demo.exe
```

Adjust the executable path for your selected CMake generator.

**Current testing limitation:** Tests are run through the executable. CTest integration, independent test registration, repeatable stress tests, and benchmark reporting are planned improvements.

## 11. Current Limitations

This project is a single-process, in-memory scheduler intended for systems-programming practice and further development.

- Tasks and dependency graphs are not persisted to disk.
- The scheduler does not distribute tasks across multiple machines.
- Running tasks cannot be forcibly cancelled.
- The priority queue is implemented as a vector scanned during task selection, so choosing a task requires a linear scan of pending work.
- Priority aging adjusts effective priority at dispatch time rather than continuously maintaining a separate priority queue.
- The DAG executor uses periodic polling to observe future completion; a completion notification mechanism would be more efficient.
- Performance has not yet been characterized through a formal benchmark suite.
- Production-grade fairness guarantees, resource limits, retries, and recovery are not implemented.

These are explicit areas for future engineering work rather than claimed capabilities.

## 12. Roadmap

### Phase 1 — Core implementation

- [x] Build a fixed-size worker pool.
- [x] Implement synchronized task submission.
- [x] Support future-based task results.
- [x] Add task lifecycle tracking and timing metrics.
- [x] Implement priority scheduling and aging.
- [x] Add pending-task cancellation.
- [x] Build dependency graphs and detect cycles.
- [x] Execute DAGs with dependency-aware scheduling.
- [x] Propagate task failures to dependent tasks.

### Phase 2 — Reliability and automated testing

- [ ] Register tests with CTest.
- [ ] Add tests for repeated construction and destruction.
- [ ] Test shutdown while work is queued and while tasks are running.
- [ ] Test cancellation races and cancellation of high-priority tasks.
- [ ] Test exceptions, empty graphs, invalid task IDs, and cyclic graphs.
- [ ] Run stress tests with many concurrent submitters.
- [ ] Investigate data races and deadlocks with suitable debugging tools.

### Phase 3 — Performance benchmarking

- [ ] Measure throughput for different worker counts.
- [ ] Measure queue wait time and task execution time.
- [ ] Compare sequential execution with thread-pool execution.
- [ ] Evaluate scheduling overhead at different queue sizes.
- [ ] Measure DAG execution with independent and dependency-heavy workloads.
- [ ] Document the machine, compiler, workload, methodology, and results.

### Phase 4 — Scheduler improvements

- [ ] Replace DAG polling with completion notifications.
- [ ] Improve cancellation and shutdown semantics.
- [ ] Consider more scalable priority-queue strategies.
- [ ] Improve API documentation and error reporting.
- [ ] Add examples and a reproducible benchmark guide.

### Phase 5 — Advanced extensions

- [ ] Add configurable retry policies for failed tasks.
- [ ] Support task deadlines and execution timeouts where cooperative cancellation permits.
- [ ] Add resource-aware scheduling or task groups.
- [ ] Explore persistence and recovery as a separate extension.

## 13. Key Concepts Demonstrated

This project provides practical experience with:

- Multithreading and concurrent programming in C++.
- Mutexes, condition variables, and shared-state synchronization.
- Futures and exception propagation.
- Work queues and scheduling policies.
- Starvation mitigation through priority aging.
- Task lifecycle and cancellation semantics.
- Directed acyclic graphs and topological sorting.
- Dependency resolution and failure propagation.
- Build management using CMake.
- Functional testing, concurrency testing, and performance analysis.

## 14. Author

**Divyansh Gupta**

- GitHub: [Divyansh-Gupta-1407](https://github.com/Divyansh-Gupta-1407)
- LinkedIn: [Profile](https://www.linkedin.com/in/gdivyansh/)

---

*This project is under active development. Features listed as implemented describe the current design; planned items are not yet part of the implementation.*
