

#ifndef YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
#define YOUR_PROJECT_NAME_PLACEHOLDERJOB_H

#include <vector>
#include <wrench-dev.h>

namespace wrench {

    class WorkflowTask;

    class PilotJob;

    class PlaceHolderJob {

    public:

        PlaceHolderJob(PilotJob *pilot_job,
                       unsigned long num_hosts,
                       std::vector<WorkflowTask *> tasks,
                       unsigned long start_level,
                       unsigned long end_level);

        PilotJob *pilot_job;
        unsigned long num_hosts;
        std::vector<WorkflowTask *> tasks;
        unsigned long start_level;
        unsigned long end_level;

        unsigned long num_standard_job_submitted = 0;

        double getDuration();

    };

};

#endif //YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
