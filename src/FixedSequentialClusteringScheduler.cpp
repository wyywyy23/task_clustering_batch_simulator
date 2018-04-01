
//
// Created by Henri Casanova on 3/30/18.
//

#include "FixedSequentialClusteringScheduler.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(fixed_clustering_scheduler, "Log category for Fixed Clustering Scheduler");

namespace wrench {

    FixedSequentialClusteringScheduler::FixedSequentialClusteringScheduler(unsigned long num_tasks_per_cluster, unsigned long max_num_submitted_jobs) {
      if (num_tasks_per_cluster < 1) {
        throw std::invalid_argument("FixedSequentialClusteringScheduler::FixedSequentialClusteringScheduler(): invalid num_tasks_per_cluster argument");
      }
      this->num_tasks_per_cluster = num_tasks_per_cluster;
      this->max_num_submitted_jobs = max_num_submitted_jobs;
    }

    void FixedSequentialClusteringScheduler::scheduleTasks(const std::set<ComputeService *> &compute_services,
                                                           const std::map<std::string, std::vector<WorkflowTask *>> &tasks) {

      if (tasks.empty()) {
        return;
      }

      TerminalOutput::setThisProcessLoggingColor(WRENCH_LOGGING_COLOR_RED);

      BatchService *batch_service = (BatchService *)(*(compute_services.begin()));

      // Create vector of ready tasks to get rid of the annoying clusters
      std::vector<WorkflowTask*> tasks_to_schedule;
      for (auto t : tasks) {
        tasks_to_schedule.push_back(*((t.second).begin()));
      }


      unsigned long first_task_in_batch = 0;
      while ((first_task_in_batch < tasks_to_schedule.size()) and (this->submitted_jobs.size() <= this->max_num_submitted_jobs)) {


        unsigned long last_task_in_batch = MIN(first_task_in_batch + this->num_tasks_per_cluster -1 , tasks.size() -1);
        unsigned long num_tasks_in_batch = last_task_in_batch - first_task_in_batch + 1;

        WRENCH_INFO("Creating a Standard job with %lu tasks:", num_tasks_in_batch);

        std::vector<WorkflowTask*> tasks_in_job;
        double total_flops = 0.0;
        WRENCH_INFO("FIRST=%lu   LAST=%lu", first_task_in_batch, last_task_in_batch);
        for (unsigned long i=first_task_in_batch; i <= last_task_in_batch; i++) {
          tasks_in_job.push_back(tasks_to_schedule[i]);
          WRENCH_INFO("  - task %s (%.2lf flops)",
                      tasks_to_schedule[i]->getId().c_str(),
                      tasks_to_schedule[i]->getFlops());
          total_flops += tasks_to_schedule[i]->getFlops();
        }

        std::map<std::string, std::string> batch_job_args;
        batch_job_args["-N"] = "1";
        batch_job_args["-t"] = std::to_string(1 + total_flops / 60.0); //time in minutes
        batch_job_args["-c"] = "1"; //number of cores per node

        StandardJob *job = this->getJobManager()->createStandardJob(tasks_in_job, {});
        WRENCH_INFO("Created a job with with batch arguments: %s:%s:%s",
            batch_job_args["-N"].c_str(),
            batch_job_args["-t"].c_str(),
            batch_job_args["-c"].c_str());

        try {
          WRENCH_INFO("Submitting the job...");
          this->getJobManager()->submitJob(
                  job, batch_service, batch_job_args);
          this->submitted_jobs.insert(job);
          WRENCH_INFO("Job submitted!");
        } catch (WorkflowExecutionException &e) {
          throw std::runtime_error("Couldn't submit job: " + e.getCause()->toString());
        }
        first_task_in_batch = last_task_in_batch + 1;
      }

      TerminalOutput::setThisProcessLoggingColor(WRENCH_LOGGING_COLOR_YELLOW);

      return;
    }

};
