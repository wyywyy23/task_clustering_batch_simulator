
//
// Created by Henri Casanova on 3/30/18.
//

#include "FixedClusteringScheduler.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(fixed_clustering_scheduler, "Log category for Fixed Clustering Scheduler");

namespace wrench {

    FixedClusteringScheduler::FixedClusteringScheduler(int num_tasks_per_cluster) {
      if (num_tasks_per_cluster < 1) {
        throw std::invalid_argument("FixedClusteringScheduler::FixedClusteringScheduler(): invalid num_tasks_per_cluster argument");
      }
      this->num_tasks_per_cluster = num_tasks_per_cluster;
    }

    void FixedClusteringScheduler::scheduleTasks(const std::set<ComputeService *> &compute_services,
                                                 const std::map<std::string, std::vector<WorkflowTask *>> &tasks) {

      if (tasks.empty()) {
        return;
      }

      WRENCH_INFO("I should be scheduling tasks here using some fixed clustering, but for now I'll"
                          "do something stupid: 1 task at a time");

      BatchService *batch_service = (BatchService *)(*(compute_services.begin()));
      WorkflowTask *task_to_schedule = *((*(tasks.begin())).second.begin());

      try {
        std::map<std::string, std::string> batch_job_args;
        batch_job_args["-N"] = "1";
        batch_job_args["-t"] = std::to_string(MAX(1, task_to_schedule->getFlops() / 60.0)); //time in minutes
        batch_job_args["-c"] = "1"; //number of cores per node

        this->getJobManager()->submitJob(
                this->getJobManager()->createStandardJob(task_to_schedule, {}),
                batch_service, batch_job_args);
      } catch (WorkflowExecutionException &e) {
        throw std::runtime_error("Couldn't submit job: " + e.getCause()->toString());
      }

      return;
    }
};