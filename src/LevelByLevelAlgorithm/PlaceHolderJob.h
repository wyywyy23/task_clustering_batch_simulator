

#ifndef YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
#define YOUR_PROJECT_NAME_PLACEHOLDERJOB_H

#include <vector>

namespace wrench {

    class WorkflowTask;
    class PilotJob;

    class PlaceHolderJob {

    public:
        PlaceHolderJob(PilotJob *pilot_job,
                       ClusteredJob *clustered_job,
                       unsigned long start_level,
                       unsigned long end_level) : pilot_job(pilot_job),
                                                  clustered_job(clustered_job),
                                                  num_completed_tasks(0),
                                                  start_level(start_level),
                                                  end_level(end_level) { }

        PilotJob *pilot_job;
        ClusteredJob *clustered_job;
        unsigned long num_completed_tasks;
        unsigned long start_level;
        unsigned long end_level;
    };

};

#endif //YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
