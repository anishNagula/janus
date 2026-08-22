#include "scheduler.h"

#include <chrono>
#include <iostream>

using namespace std;

Scheduler::Scheduler():
  queues(NUM_LEVELS),
  shutdown(false),
  next_job_id(1) {}

Scheduler::~Scheduler() {
  stop();
}



void Scheduler::start() {
  shutdown = false;

  scheduler_thread = thread(&Scheduler::scheduler_thread, this);
  boost_thread = thread(&Scheduler::boost_thread, this);
}



void Scheduler::stop() {
  if (shutdown) return;

  shutdown = true;

  cv.notify_all();  // wakes sleeping threads to read if (shutdown) break

  if (scheduler_thread.joinable()) scheduler_thread.join();   // wait for scheduler_thread to finish
  if (boost_thread.joinable()) boost_thread.join();           // wait for boost_thread to finish
}



int Scheduler::submitJob(int work) {
  lock_guard<mutex> lock(queue_mutex);

  int id = next_job_id++;

  Job job(id, work);

  queues[0].push(job);

  cout
    << "[SUBMIT] Job " << id
    << " -> Q0"
    << " | work=" << work
    << '\n';


    cv.notify_one();      // wake scheduler if sleeping

    return id;
}



bool Scheduler::hasJobs() {
  for (const auto &queue : queues) {
    if (!queue.empty()) return true;
  }

  return false;
}



Job Scheduler::getNextJob() {

  // queue_mutex must already be locked

  for (int level = 0; level < NUM_LEVELS; ++level) {
    if (!queues[level].empty()) {
      Job job = queues[level].front();

      queues[level].pop();
      return job;
    }
  }

  return Job(-1, 0);      // edge case return job with id : -1 and work : 0

}

void Scheduler::runJob(Job job) {

  int level = job.priority;

  job.state = JobState::RUNNING;

  int execution_time = min(quantum[level], job.remaining_work);   // job gets atmost one quantum during this scheduling turn

  cout
    << "[RUN] Job " << job.id
    << " | Q" << level
    << " | running=" <<  execution_time
    << '\n';

  
  // simulating cpu
  this_thread::sleep_for(
    chrono::milliseconds(execution_time)
  );

  job.remaining_work -= execution_time;
  job.level_runtime += execution_time;

  if (job.remaining_work <= 0) {
    job.state = JobState::COMPLETED;

    cout
      << "[COMPLETED] Job "
      << job.id
      << '\n';

      return;
  }


  if (job.level_runtime >= allotment[level]) {        // rule 4 (demote after level_runtime >= allotment time)
    demote(job);
  } else {                                            // rule 2 (same level round robin)
    lock_guard<mutex> lock(queue_mutex);
    job.state = JobState::READY;
    queues[level].push(job);

    cout
      << "[REQUEUE] Job " << job.id
      << " -> Q" << level
      << '\n';
  }
}



void Scheduler::demote(Job &job) {

  lock_guard<mutex> lock(queue_mutex);

  if (job.priority < NUM_LEVELS - 1) {
    int old_priority = job.priority;

    job.priority++;

    job.level_runtime = 0;
    job.state = JobState::READY;

    queues[job.priority].push(job);

    cout
      << "[DEMOTE] Job " << job.id
      << " Q" << old_priority
      << " -> Q" << job.priority
      << '\n';
  } else {                                            // already in lowest level

    job.level_runtime = 0;
    job.state = JobState::READY;

    queues[job.priority].push(job);

    cout
      << "[REQUEUE] Job " << job.id
      << " remains Q" << job.priority
      << '\n';
  }
}



void Scheduler::schedulerLoop() {

  

}