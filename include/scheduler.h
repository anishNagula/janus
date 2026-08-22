#ifndef SCHEDULER_H
#define SCHEDULER_H

#include "job.h"

#include <queue>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

using namespace std;

class Scheduler{

private:

  static constexpr int NUM_LEVELS = 3;   // #levels

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

  vector<queue<Job>> queues;

  mutex queue_mutex;            // protects queue
  condition_variable cv;        // sleep scheduler until work available
  atomic<bool> shutdown;        // shared flag b/w producer, scheduler, booster

  thread scheduler_thread;      // run scheduler
  thread boost_thread;          // run booster

  int next_job_id;

  bool hasJobs();

  Job getNextJob();

  void runJob(Job job);
  void demote(Job &job);

  void boostPriorities();

  void schedulerLoop();
  void boostLoop();



public:

  Scheduler();

  ~Scheduler();

  void start();
  void stop();

  int submitJob(int work);
};


#endif