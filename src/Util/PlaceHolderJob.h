

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
                       unsigned long end_level);

        PilotJob *pilot_job;
        std::vector<WorkflowTask *> tasks;
        unsigned long start_level;
        unsigned long end_level;

        double getDuration();
    };

};

#endif //YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
