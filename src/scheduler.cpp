#include "scheduler.h"

#include <chrono>
#include <iostream>
#include <iomanip>
#include <ctime>
#include <sstream>

using namespace std;

Scheduler::Scheduler():
    queues(NUM_LEVELS),
    shutdown(false),
    next_job_id(1) {}

Scheduler::~Scheduler() {
    stop();
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

    if (scheduler_thread.joinable()) scheduler_thread.join();   // wait for scheduler_thread to finish
    if (boost_thread.joinable()) boost_thread.join();           // wait for boost_thread to finish
}


// ------------ Logging ------------

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



void Scheduler::log(const string& message) {
    lock_guard<mutex> lock(print_mutex);

    cout << "[" << currentTime() << "] " << message << '\n';
}


// ------------ Schedule ~ Check ~ getNext Jobs ------------

int Scheduler::submitJob(int work) {
    lock_guard<mutex> lock(queue_mutex);

    int id = next_job_id++;

    Job job(id, work);

    queues[0].push(job);

    log(
        "[SUBMIT] Job " + std::to_string(id) +
        " -> Q0 | work=" + std::to_string(work)
    );

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

// ------------ Run Job ------------

void Scheduler::runJob(Job job) {

    int level = job.priority;

    job.state = JobState::RUNNING;

    int execution_time = min(quantum[level], job.remaining_work);   // job gets atmost one quantum during this scheduling turn

    log(
        "[RUN] Job " + std::to_string(job.id) +
        " | Q" + std::to_string(level) +
        " | running=" + std::to_string(execution_time) + "ms"
    );

    
    // simulating cpu
    this_thread::sleep_for(
        chrono::milliseconds(execution_time)
    );

    job.remaining_work -= execution_time;
    job.level_runtime += execution_time;

    if (job.remaining_work <= 0) {
        job.state = JobState::COMPLETED;

        log(
            "[COMPLETED] Job " +
            std::to_string(job.id)
        );

        return;
    }


    if (job.level_runtime >= allotment[level]) {        // rule 4 (demote after level_runtime >= allotment time)
        demote(job);
    } else {                                            // rule 2 (same level round robin)
        lock_guard<mutex> lock(queue_mutex);
        job.state = JobState::READY;
        queues[level].push(job);

        log(
            "[REQUEUE] Job " + std::to_string(job.id) +
            " -> Q" + std::to_string(level)
        );
    }
}

// ------------ Demote Logic ------------

void Scheduler::demote(Job &job) {

    lock_guard<mutex> lock(queue_mutex);

    if (job.priority < NUM_LEVELS - 1) {
        int old_priority = job.priority;

        job.priority++;

        job.level_runtime = 0;
        job.state = JobState::READY;

        queues[job.priority].push(job);

        log(
            "[DEMOTE] Job " + std::to_string(job.id) +
            " Q" + std::to_string(old_priority) +
            " -> Q" + std::to_string(job.priority)
        );
    } else {                                            // already in lowest level

        job.level_runtime = 0;
        job.state = JobState::READY;

        queues[job.priority].push(job);

        log(
            "[REQUEUE] Job " + std::to_string(job.id) +
            " remains Q" + std::to_string(job.priority)
        );
    }
}

// ------------ Scheduler and Boost Loop ------------

void Scheduler::schedulerLoop() {

    while (!shutdown) {
        
        Job job(-1, 0);

        {
        unique_lock<mutex> lock(queue_mutex);

        cv.wait(lock, [this] {                          // sleep until job available or scheduler shutdown
            return shutdown || hasJobs();
        });

        if (shutdown) break;

        job = getNextJob();                            // scan from q0 (high priority)
        }

        runJob(job);   //  dont hold lock while exec so that other jobs can be submitted
    }  
}

void Scheduler::boostPriorities() {
    lock_guard<mutex> lock(queue_mutex);

    queue<Job> boosted;

    for (int level = 0; level < NUM_LEVELS; level++) {
        while (!queues[level].empty()) {
        Job job = queues[level].front();
        queues[level].pop();

        job.priority = 0;
        job.level_runtime = 0;
        job.state = JobState::READY;

        boosted.push(job);
        }
    }

    queues[0] = std::move(boosted);

    if (!queues[0].empty()) {
        log(
            "[BOOST] All waiting jobs moved to Q0"
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
        
        if (shutdown) break;

        lock.unlock();

        boostPriorities();

        lock.lock();
    }
}