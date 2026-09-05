#include "job.h"

#include <utility>

Job::Job(int id, TaskFunction task):
    id(id),
    task(std::move(task)),
    priority(0),
    level_runtime(0),
    state(JobState::READY) {}

bool Job::isCompleted() const {
    return state == JobState::COMPLETED;
}