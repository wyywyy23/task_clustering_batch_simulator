

#ifndef YOUR_PROJECT_NAME_EVANPLACEHOLDERJOB_H
#define YOUR_PROJECT_NAME_EVANPLACEHOLDERJOB_H

#include <vector>

namespace wrench {

    class WorkflowTask;
    class PilotJob;

    class EvanPlaceHolderJob {

    public:
        EvanPlaceHolderJob(PilotJob *pilot_job,
            std::vector<WorkflowTask *> tasks,
            unsigned long start_level,
            unsigned long end_level) : pilot_job(pilot_job),
            tasks(tasks),
            start_level(start_level),
            end_level(end_level) { }

            PilotJob *pilot_job;
            std::vector<WorkflowTask *> tasks;
            unsigned long start_level;
            unsigned long end_level;
        };

    };

    #endif //YOUR_PROJECT_NAME_EVANPLACEHOLDERJOB_H
