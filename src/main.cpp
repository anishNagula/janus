#include "../include/scheduler.h"

#include <iostream>
#include <chrono>
#include <thread>

int main() {

    Scheduler scheduler;

    scheduler.start();


    thread producer1([&scheduler]() {
        
        scheduler.submitJob(1000);
        scheduler.submitJob(500);
        scheduler.submitJob(800);
    });

    thread producer2([&scheduler]() {

        scheduler.submitJob(300);
        scheduler.submitJob(1200);
        scheduler.submitJob(600);
    });

    thread producer3([&scheduler]() {

        scheduler.submitJob(700);
        scheduler.submitJob(400);
        scheduler.submitJob(1500);
    });

    producer1.join();
    producer2.join();
    producer3.join();


    this_thread::sleep_for(chrono::seconds(20));

    scheduler.stop();

    cout << "\nScheduler stopped.\n";

    return 0;
}