

#ifndef YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
#define YOUR_PROJECT_NAME_PLACEHOLDERJOB_H

#include <workflow/job/PilotJob.h>

namespace wrench {

    class WorkflowTask;

    class PlaceHolderJob {

    public:
        PlaceHolderJob(PilotJob *pilot_job,
                       std::vector<WorkflowTask *> tasks,
                       unsigned long start_level,
                       unsigned long end_level) : pilot_job(pilot_job),
                                                  tasks(tasks),
                                                  num_completed_tasks(0),
                                                  start_level(start_level),
                                                  end_level(end_level) { }

        PilotJob *pilot_job;
        std::vector<WorkflowTask *> tasks;
        unsigned long num_completed_tasks;
        unsigned long start_level;
        unsigned long end_level;
    };

};

#endif //YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
