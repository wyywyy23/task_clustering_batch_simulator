
//
// Created by Henri Casanova on 3/30/18.
//

#include <WorkflowUtil/WorkflowUtil.h>
#include "FixedClusteringScheduler.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(fixed_clustering_scheduler, "Log category for Fixed Clustering Scheduler");

#define EXECUTION_TIME_FUDGE_FACTOR 60

namespace wrench {

    FixedClusteringScheduler::FixedClusteringScheduler(
            unsigned long num_tasks_per_cluster,
            unsigned long num_nodes_per_cluster,
            unsigned long max_num_submitted_jobs) {
      if ((num_tasks_per_cluster < 1) || (num_nodes_per_cluster < 1) || (max_num_submitted_jobs < 1)) {
        throw std::invalid_argument("FixedClusteringScheduler::FixedClusteringScheduler(): invalid arguments");
      }
      this->core_speed = -1.0;

      this->num_tasks_per_cluster = num_tasks_per_cluster;
      this->num_nodes_per_cluster = num_nodes_per_cluster;
      this->max_num_submitted_jobs = max_num_submitted_jobs;
    }

    void FixedClusteringScheduler::scheduleTasks(const std::set<ComputeService *> &compute_services,
                                                           const std::map<std::string, std::vector<WorkflowTask *>> &tasks) {

      if (tasks.empty()) {
        return;
      }

      BatchService *batch_service = (BatchService *)(*(compute_services.begin()));

      // Acquire core speed the first time
      if (this->core_speed < 0.0) {
        this->core_speed = batch_service->getCoreFlopRate()[0];
      }

      TerminalOutput::setThisProcessLoggingColor(COLOR_RED);


      // Create vector of ready tasks to get rid of the annoying clusters
      std::vector<WorkflowTask*> tasks_to_schedule;
      for (auto t : tasks) {
        tasks_to_schedule.push_back(*((t.second).begin()));
      }


      unsigned long first_task_in_batch = 0;
      while ((first_task_in_batch < tasks_to_schedule.size()) and (this->submitted_jobs.size() < this->max_num_submitted_jobs)) {


        unsigned long last_task_in_batch = MIN(first_task_in_batch + this->num_tasks_per_cluster -1 , tasks.size() -1);
        unsigned long num_tasks_in_batch = last_task_in_batch - first_task_in_batch + 1;

        WRENCH_INFO("Creating a Standard job with %lu tasks:", num_tasks_in_batch);

        std::vector<WorkflowTask*> tasks_in_job;
        for (unsigned long i=first_task_in_batch; i <= last_task_in_batch; i++) {
          tasks_in_job.push_back(tasks_to_schedule[i]);
          WRENCH_INFO("  - task %s (%.2lf flops)",
                      tasks_to_schedule[i]->getId().c_str(),
                      tasks_to_schedule[i]->getFlops());
        }

        // Compute the number of nodes for the job
        unsigned long num_nodes = MIN(num_tasks_in_batch, this->num_nodes_per_cluster);

        // Compute the time for the job (a bit conservative for now)
//        double max_flop_best_fit_time = computeJobTime(num_nodes, tasks_in_job);
        double makespan = WorkflowUtil::estimateMakespan(tasks_in_job, num_nodes, this->core_speed);

        std::map<std::string, std::string> batch_job_args;
        batch_job_args["-N"] = std::to_string(num_nodes);
        batch_job_args["-t"] = std::to_string((unsigned long)(1 + (makespan + EXECUTION_TIME_FUDGE_FACTOR) / 60.0)); //time in minutes
        batch_job_args["-c"] = "1"; //number of cores per node

        StandardJob *job = this->getJobManager()->createStandardJob(tasks_in_job, {});
        WRENCH_INFO("Created a batch job with with batch arguments: %s:%s:%s",
            batch_job_args["-N"].c_str(),
            batch_job_args["-t"].c_str(),
            batch_job_args["-c"].c_str());

        try {
          WRENCH_INFO("Submitting batch job...");
          this->getJobManager()->submitJob(
                  job, batch_service, batch_job_args);
          this->submitted_jobs.insert(job);
          WRENCH_INFO("Batch job submitted!");
        } catch (WorkflowExecutionException &e) {
          throw std::runtime_error("Couldn't submit job: " + e.getCause()->toString());
        }
        first_task_in_batch = last_task_in_batch + 1;
        WRENCH_INFO("----> ftib: %ld\n", first_task_in_batch);
      }

      WRENCH_INFO("Done with scheduling decisions");
      TerminalOutput::setThisProcessLoggingColor(COLOR_YELLOW);

      return;
    }


};
