#ifndef SCHEDULER_H
#define SCHEDULER_H

#include "job.h"

#include <queue>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <string>
#include <unordered_map>
#include <chrono>

using namespace std;

class Scheduler{

private:

    static constexpr int NUM_LEVELS = 3;   // #levels

    vector<queue<JobHandle>> queues;
    unordered_map<JobHandle, shared_ptr<Job>> jobs;     // all jobs in system

    mutex queue_mutex;
    condition_variable cv;

    atomic<bool> shutdown;

    int next_job_id;

    thread scheduler_thread;
    thread boost_thread;

    condition_variable scheduler_cv;
    mutex scheduler_mutex;

    const int quantum[NUM_LEVELS] = {
      100,
      200,
      400,
    };

    const int allotment[NUM_LEVELS] = {
      400,
      800,
      1600
    };

    void schedulerLoop();
    void boostLoop();

    void runJob(JobHandle id);

    void jobThread(JobHandle id);

    void demote(shared_ptr<Job> job);

    void boostPriorities();

    bool hasJobs();

    JobHandle getNextJob();

    shared_ptr<Job> getJob(JobHandle id);

    void notifyScheduler();

    static string stateToString(JobState state);

public:

    Scheduler();
    ~Scheduler();

    void start();
    void stop();

    JobHandle fork_job(TaskFunction task);

    void yield();
    void wait(JobHandle id);
    void sleep_for(int milliseconds);
    void exit_job();

    void log(const string& message);
};


#endif