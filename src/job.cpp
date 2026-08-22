#include "job.h"

Job::Job(int id, int work):
  id(id),
  total_work(work),
  remaining_work(work),
  priority(0),
  level_runtime(0),
  state(JobState::READY) {}

bool Job::isCompleted() const {
  return remaining_work <= 0;
}