#include "../include/scheduler.h"

#include <chrono>
#include <iostream>
#include <thread>

using namespace std;

int main() {

    Scheduler scheduler;

    scheduler.start();

    // --------------------------------------------------------
    // Job 1
    // --------------------------------------------------------

    JobHandle job1 =
        scheduler.fork_job([&scheduler]() {

            for (int i = 1; i <= 5; ++i) {

                cout << "Job 1: step "
                     << i << '\n';

                this_thread::sleep_for(
                    chrono::milliseconds(100)
                );

                // Give scheduler control.
                scheduler.yield();
            }

            scheduler.exit_job();
        });


    // --------------------------------------------------------
    // Job 2
    // --------------------------------------------------------

    JobHandle job2 =
        scheduler.fork_job([&scheduler]() {

            for (int i = 1; i <= 5; ++i) {

                cout << "Job 2: step "
                     << i << '\n';

                this_thread::sleep_for(
                    chrono::milliseconds(100)
                );

                scheduler.yield();
            }

            scheduler.exit_job();
        });


    // --------------------------------------------------------
    // Job 3 demonstrates wait()
    // --------------------------------------------------------

    JobHandle job3 =
        scheduler.fork_job(
            [&scheduler, job1]() {

                cout << "Job 3: waiting for Job 1\n";

                scheduler.wait(job1);

                cout << "Job 3: Job 1 completed\n";

                for (int i = 1; i <= 3; ++i) {

                    cout << "Job 3: step "
                         << i << '\n';

                    scheduler.yield();
                }

                scheduler.exit_job();
            }
        );


    // --------------------------------------------------------
    // Job 4 demonstrates sleep_for()
    // --------------------------------------------------------

    JobHandle job4 =
        scheduler.fork_job(
            [&scheduler]() {

                cout << "Job 4: starting\n";

                scheduler.sleep_for(1000);

                cout << "Job 4: woke up\n";

                scheduler.yield();

                cout << "Job 4: finishing\n";

                scheduler.exit_job();
            }
        );


    // Give jobs enough time to execute.
    this_thread::sleep_for(
        chrono::seconds(8)
    );


    scheduler.stop();

    cout << "\nScheduler stopped.\n";

    return 0;
}