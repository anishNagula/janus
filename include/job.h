#ifndef JOB_H
#define JOB_H

#include <string>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <thread>

enum JobState {
    READY,
    RUNNING,
    BLOCKED,
    COMPLETED
};

using JobHandle = int;
using TaskFunction = std::function<void()>;

struct Job {
    Job(JobHandle id, TaskFunction task);

    bool isCompleted() const;

    JobHandle id;
    TaskFunction task;

    int priority; // starts off with 0 (highest)
    int level_runtime;  // current cpu time spent in level

    JobState state;

    // used to control job's execution
    std::mutex mutex;
    std::condition_variable cv;

    // scheduler gives job permission to run
    bool can_run = false;

    // job notifies scheduler of yield (blocked / completed)
    bool scheduler_notified = false;

    std::thread worker;

    // used by sleep_for()
    bool sleeping = false;
};

#endif