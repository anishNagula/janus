#include "scheduler.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

using namespace std;

thread_local JobHandle current_job_id = -1;
thread_local Scheduler* current_scheduler = nullptr;

// ------------ TimeStamp ------------

string currentTime() {

    auto now = chrono::system_clock::now();
    auto time = chrono::system_clock::to_time_t(now);

    tm localTime = *localtime(&time);

    auto ms = chrono::duration_cast<chrono::milliseconds>(
        now.time_since_epoch()
    ) % 1000;

    ostringstream oss;

    oss << put_time(&localTime, "%H:%M:%S")
        << '.'
        << setfill('0') << setw(3) << ms.count();

    return oss.str();
}



Scheduler::Scheduler():
    queues(NUM_LEVELS),
    shutdown(false),
    next_job_id(1) {}

Scheduler::~Scheduler() {
    stop();
}


// ------------ Logging ------------

void Scheduler::log(const string& message) {
    static mutex print_mutex;
    lock_guard<mutex> lock(print_mutex);

    cout << "[" << currentTime() << "] " << message << '\n';
}



// ------------ Start and Stop ------------

void Scheduler::start() {
    shutdown = false;

    scheduler_thread = thread(&Scheduler::schedulerLoop, this);
    boost_thread = thread(&Scheduler::boostLoop, this);
}

void Scheduler::stop() {
    if (shutdown) return;

    shutdown = true;

    cv.notify_all();  // wakes sleeping threads to read if (shutdown) break
    scheduler_cv.notify_all();

    // wake up every job
    {
        lock_guard<mutex> lock(queue_mutex);

        for (auto& [id, job] : jobs) {
            lock_guard<mutex> job_lock(job->mutex);

            job->can_run = true;
            job->cv.notify_all();
        }
    }

    if (scheduler_thread.joinable()) scheduler_thread.join();   // wait for scheduler_thread to finish
    if (boost_thread.joinable()) boost_thread.join();           // wait for boost_thread to finish

    // join all job threads
    for (auto& [id, job] : jobs) {
        if (job->worker.joinable()) job->worker.join();
    }
}


// ------------ Fork Job ------------

JobHandle Scheduler::fork_job(TaskFunction task) {

    lock_guard<mutex> lock(queue_mutex);

    JobHandle id = next_job_id++;

    auto job = make_shared<Job>(id, std::move(task));

    jobs[id] = job;

    queues[0].push(id);

    log(
        "[FORK] Job " +
        to_string(id) +
        " -> Q0"
    );

    // create job's execution thread
    job->worker = thread(&Scheduler::jobThread, this, id);

    cv.notify_one();

    return id;
}


// ------------ Get Job ------------

shared_ptr<Job> Scheduler::getJob(JobHandle id) {

    lock_guard<mutex> lock(queue_mutex);

    auto it = jobs.find(id);

    if (it == jobs.end()) return nullptr;

    return it->second;
}



// ------------ Query Helpers ------------



bool Scheduler::hasJobs() {
    for (const auto &queue : queues) {
        if (!queue.empty()) return true;
    }

    return false;
}



JobHandle Scheduler::getNextJob() {

    // queue_mutex must already be locked

    for (int level = 0; level < NUM_LEVELS; ++level) {
        if (!queues[level].empty()) {
        JobHandle id = queues[level].front();

        queues[level].pop();
        return id;
        }
    }

    return -1;      // edge case return job with id : -1 and work : 0
}



// ------------ Run Job ------------

void Scheduler::runJob(JobHandle id) {

    auto job = getJob(id);

    if (!job) return;

    {
        lock_guard<mutex> lock(job->mutex);

        if (job->state == JobState::COMPLETED) return;

        job->state = JobState::RUNNING;
        job->scheduler_notified = false;
        job->can_run = true;

        job->cv.notify_one();
    }

    log(
        "[RUN] Job " +
        to_string(id) +
        " | Q" +
        to_string(job->priority)
    );

    // wait until job yields/blocks/completes/sleeps
    unique_lock<mutex> lock(scheduler_mutex);

    scheduler_cv.wait(lock, [this, &job] {
        return shutdown || job->scheduler_notified;
    });

    job->scheduler_notified = false;
}


// ------------ Job Thread ------------

void Scheduler::jobThread(JobHandle id) {

    auto job = getJob(id);

    if (!job) return;

    current_job_id = id;
    current_scheduler = this;


    // wait until scheduler dispatches us
    {
        unique_lock<mutex> lock(job->mutex);

        job->cv.wait(lock, [&] {
            return job->can_run || shutdown;
        });

        if (shutdown) return;

        job->can_run = false;
    }

    log(
        "[START] Job " +
        to_string(id)
    );

    try {
        job->task();
    }
    catch (...) {

        log(
            "[ERROR] Job " +
            to_string(id) +
            " threw an exception"
        );
    }


    {
        lock_guard<mutex> lock(job->mutex);

        if (job->state != JobState::COMPLETED) {
            job->state = JobState::COMPLETED;

            log(
                "[COMPLETED] Job " +
                to_string(id)
            );
        }

        job->scheduler_notified = true;
    }

    scheduler_cv.notify_one();
}


// ------------ Demote Logic ------------

void Scheduler::demote(shared_ptr<Job> job) {

    lock_guard<mutex> lock(queue_mutex);

    int old_priority =
        job->priority;

    if (job->priority < NUM_LEVELS - 1) {

        job->priority++;

        job->level_runtime = 0;

        job->state = JobState::READY;

        queues[job->priority].push(job->id);

        log(
            "[DEMOTE] Job " +
            to_string(job->id) +
            " Q" +
            to_string(old_priority) +
            " -> Q" +
            to_string(job->priority)
        );

        cv.notify_one();
    }
    else {

        // Already at lowest level.
        job->level_runtime = 0;

        job->state = JobState::READY;

        queues[job->priority].push(job->id);

        log(
            "[REQUEUE] Job " +
            to_string(job->id) +
            " remains Q" +
            to_string(job->priority)
        );

        cv.notify_one();
    }
}



// ------------ Yield ------------
void Scheduler::yield() {
    if (current_job_id == -1 || current_scheduler == nullptr) {
        return;
    }

    auto job = current_scheduler->getJob(current_job_id);

    if (!job) return;

    {
        lock_guard<mutex> lock(job->mutex);

        job->state = JobState::READY;

        job->scheduler_notified = true;

        log(
            "[YIELD] Job " +
            to_string(job->id)
        );
    }

    current_scheduler->scheduler_cv.notify_one();

    {
        unique_lock<mutex> lock(job->mutex);

        job->cv.wait(lock, [&] {
            return job->can_run || current_scheduler->shutdown;
        });

        if (current_scheduler->shutdown) return;

        job->can_run = false;
        job->state = JobState::RUNNING;
    }
}


// ------------ Wait ------------
void Scheduler::wait(JobHandle id) {

    if (current_job_id == id) return;

    auto target = getJob(id);

    if (!target) return;

    log(
        "[WAIT] Job " +
        to_string(current_job_id) +
        " waiting for Job " +
        to_string(id)
    );

    {
        lock_guard<mutex> lock(target->mutex);

        if (target->state == JobState::COMPLETED) return;
    }

    auto current = getJob(current_job_id);

    if (!current) return;

    {
        lock_guard<mutex> lock(current->mutex);

        current->state = JobState::BLOCKED;
        current->scheduler_notified = true;
    }

    scheduler_cv.notify_one();

    unique_lock<mutex> target_lock(target->mutex);

    target->cv.wait(target_lock, [&] {
        return target->state == JobState::COMPLETED || shutdown;
    });

    if (shutdown) return;

    {
        lock_guard<mutex> lock(current->mutex);

        current->state = JobState::RUNNING;
    }


    log(
        "[WAIT-DONE] Job " +
        to_string(current_job_id) +
        " resumed"
    );

}


// ------------ Sleep For ------------
void Scheduler::sleep_for(int milliseconds) {

    if (current_job_id == -1 ||
        current_scheduler == nullptr)
        return;

    auto job =
        current_scheduler->getJob(current_job_id);

    if (!job)
        return;

    log(
        "[SLEEP] Job " +
        to_string(job->id) +
        " for " +
        to_string(milliseconds) +
        "ms"
    );

    {
        lock_guard<mutex> lock(job->mutex);

        job->state = JobState::BLOCKED;
        job->sleeping = true;
        job->scheduler_notified = true;
    }

    scheduler_cv.notify_one();

    // Sleep outside the scheduler lock.
    this_thread::sleep_for(
        chrono::milliseconds(milliseconds)
    );

    {
        lock_guard<mutex> lock(job->mutex);

        job->sleeping = false;

        if (job->state != JobState::COMPLETED)
            job->state = JobState::RUNNING;
    }

    log(
        "[WAKE] Job " +
        to_string(job->id)
    );

}


// ------------ Exit ------------
void Scheduler::exit_job() {

    if (current_job_id == -1 ||
        current_scheduler == nullptr)
        return;

    auto job =
        current_scheduler->getJob(current_job_id);

    if (!job)
        return;

    {
        lock_guard<mutex> lock(job->mutex);

        job->state = JobState::COMPLETED;
        job->scheduler_notified = true;
    }

    log(
        "[EXIT] Job " +
        to_string(job->id)
    );

    scheduler_cv.notify_one();
}



// ------------ Scheduler and Boost Loop ------------

void Scheduler::schedulerLoop() {

    while (!shutdown) {

        JobHandle id = -1;

        {
            unique_lock<mutex> lock(queue_mutex);

            cv.wait(lock, [this] {
                return shutdown || hasJobs();
            });

            if (shutdown)
                break;

            id = getNextJob();
        }

        if (id != -1)
            runJob(id);
    }
}


void Scheduler::boostPriorities() {

    lock_guard<mutex> lock(queue_mutex);

    queue<JobHandle> boosted;

    for (int level = 0;
         level < NUM_LEVELS;
         ++level) {

        while (!queues[level].empty()) {

            JobHandle id =
                queues[level].front();

            queues[level].pop();

            auto it = jobs.find(id);

            if (it == jobs.end())
                continue;

            auto job = it->second;

            lock_guard<mutex> job_lock(job->mutex);

            if (job->state != JobState::COMPLETED) {

                job->priority = 0;
                job->level_runtime = 0;
                job->state = JobState::READY;

                boosted.push(id);
            }
        }
    }

    queues[0] = std::move(boosted);

    if (!queues[0].empty()) {

        log(
            "[BOOST] All ready jobs moved to Q0"
        );

        cv.notify_one();
    }
}



void Scheduler::boostLoop() {

    constexpr int BOOST_INTERVAL = 5000;

    unique_lock<mutex> lock(queue_mutex);

    while (!shutdown) {

        cv.wait_for(
            lock,
            chrono::milliseconds(BOOST_INTERVAL),
            [this] {
                return shutdown.load();
            }
        );

        if (shutdown)
            break;

        lock.unlock();

        boostPriorities();

        lock.lock();
    }
}