//
// Created by Henri Casanova on 3/30/18.
//

#ifndef TASK_CLUSTERING_FOR_BATCH_FIXEDCLUSTERINGSCHEDULER_H
#define TASK_CLUSTERING_FOR_BATCH_FIXEDCLUSTERINGSCHEDULER_H

#include <wrench-dev.h>

namespace wrench {

    /**
     * @brief A batch Scheduler
     */
    class FixedSequentialClusteringScheduler : public StandardJobScheduler {

    public:

        FixedSequentialClusteringScheduler(unsigned long num_tasks_per_cluster, unsigned long max_num_submitted_jobs);

        void scheduleTasks(const std::set<ComputeService *> &compute_services,
                           const std::map<std::string, std::vector<WorkflowTask *>> &tasks) override;

        std::set<StandardJob*> submitted_jobs;

    private:
        unsigned long num_tasks_per_cluster;
        unsigned long max_num_submitted_jobs;

    };
}


#endif //TASK_CLUSTERING_FOR_BATCH_FIXEDCLUSTERINGSCHEDULER_H
