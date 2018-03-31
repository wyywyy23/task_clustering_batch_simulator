//
// Created by Henri Casanova on 3/30/18.
//

#ifndef YOUR_PROJECT_NAME_FIXEDCLUSTERINGSCHEDULER_H
#define YOUR_PROJECT_NAME_FIXEDCLUSTERINGSCHEDULER_H

#include <wrench-dev.h>

namespace wrench {

    /**
     * @brief A batch Scheduler
     */
    class FixedClusteringScheduler : public StandardJobScheduler {

    public:

        FixedClusteringScheduler(int num_tasks_per_cluster);

        void scheduleTasks(const std::set<ComputeService *> &compute_services,
                           const std::map<std::string, std::vector<WorkflowTask *>> &tasks) override;

    private:
        int num_tasks_per_cluster;

    };
}


#endif //YOUR_PROJECT_NAME_FIXEDCLUSTERINGSCHEDULER_H
