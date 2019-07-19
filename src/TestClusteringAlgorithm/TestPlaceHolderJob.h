

#ifndef YOUR_PROJECT_NAME_EVANPLACEHOLDERJOB_H
#define YOUR_PROJECT_NAME_EVANPLACEHOLDERJOB_H

#include <vector>

namespace wrench {

    class WorkflowTask;
    class PilotJob;

    class TestPlaceHolderJob {

    public:
        TestPlaceHolderJob(PilotJob *pilot_job,
                           unsigned long num_nodes,
                           std::vector<WorkflowTask *> tasks,
                           unsigned long start_level,
                           unsigned long end_level) : pilot_job(pilot_job),
                                                      num_nodes(num_nodes),
                                                      start_level(start_level),
                                                      end_level(end_level) {

            this->tasks = tasks;
            // Sort the tasks by decreasing flops!
            std::sort(this->tasks.begin(), this->tasks.end(),
                      [](const WorkflowTask*  t1, const WorkflowTask*  t2) -> bool
                      {
                          if (t1->getFlops() == t2->getFlops()) {
                              return (t1->getID() >  t2->getID());
                          }
                          return (t1->getFlops() >= t2->getFlops());

                      });

        }

        PilotJob *pilot_job;
        unsigned long num_nodes;
        std::vector<WorkflowTask *> tasks;
        unsigned long start_level;
        unsigned long end_level;
        unsigned long num_currently_running_tasks = 0;
    };

};

#endif //YOUR_PROJECT_NAME_EVANPLACEHOLDERJOB_H
