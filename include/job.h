#ifndef JOB_H
#define JOB_H

#include <string>

enum JobState {
  READY,
  RUNNING,
  COMPLETED
};

struct Job {
  int id; // pid

  int total_work;
  int remaining_work;

  int priority; // starts off with 0 (highest)
  int level_runtime;  // current cpu time spent in level

  JobState state;

  Job(int id, int work);

  bool isCompleted() const;
};

#endif