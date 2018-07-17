

#ifndef YOUR_PROJECT_NAME_ZHANGPLACEHOLDERJOB_H
#define YOUR_PROJECT_NAME_ZHANGPLACEHOLDERJOB_H

#include <vector>

namespace wrench {

    class WorkflowTask;
    class PilotJob;

    class ZhangPlaceHolderJob {

    public:
        ZhangPlaceHolderJob(PilotJob *pilot_job,
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

#endif //YOUR_PROJECT_NAME_ZHANGPLACEHOLDERJOB_H
