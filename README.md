# Distributed MLFQ Task Scheduler

A C++ task scheduling runtime based on the Multi-Level Feedback Queue (MLFQ) scheduling algorithm.

The scheduler manages concurrent jobs using multiple priority queues and coordinates their execution according to MLFQ scheduling rules.

## Features

- Multi-Level Feedback Queue scheduling
- Round-robin scheduling within each priority level
- Per-level time quanta and time allotments
- Automatic priority demotion
- Periodic priority boosting
- Concurrent job submission
- Thread-safe scheduling using mutexes
- Condition variables for efficient thread synchronization
- Atomic shutdown handling
- Multithreaded scheduler and priority boost mechanisms

## MLFQ Scheduling Rules

1. Higher-priority jobs run before lower-priority jobs.
2. Jobs at the same priority level execute in round-robin order.
3. New jobs enter the highest-priority queue.
4. A job is demoted after consuming its time allotment at its current level.
5. After a fixed interval, all waiting jobs are moved to the highest-priority queue.

## Architecture

```text
                    Clients
                       |
                       v
              +----------------+
              |    Scheduler   |
              |                |
              | Q0             |
              | Q1             |
              | Q2             |
              +-------+--------+
                      |
                      v
               Job Execution
```

## Technologies
- C++17
- CMake
- POSIX/Linux threads
- std::thread
- std::mutex
- std::condition_variable
- std::atomic

## Build
```
mkdir build
cd build
cmake ..
make
```

## Run
```
./janus
```