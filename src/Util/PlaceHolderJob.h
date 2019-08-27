

#ifndef YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
#define YOUR_PROJECT_NAME_PLACEHOLDERJOB_H

#include <vector>

namespace wrench {

    class WorkflowTask;

    class PilotJob;

    class PlaceHolderJob {

    public:
        PlaceHolderJob(PilotJob *pilot_job,
                       std::vector<WorkflowTask *> tasks,
                       unsigned long start_level,
                       unsigned long end_level,
                       double requested_execution_time) : pilot_job(pilot_job),
                                                          tasks(tasks),
                                                          start_level(start_level),
                                                          end_level(end_level),
                                                          requested_execution_time(requested_execution_time) {}

        PilotJob *pilot_job;
        std::vector<WorkflowTask *> tasks;
        unsigned long start_level;
        unsigned long end_level;
        double requested_execution_time;
    };

};

#endif //YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
