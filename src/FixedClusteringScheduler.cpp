
//
// Created by Henri Casanova on 3/30/18.
//

#include "FixedClusteringScheduler.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(fixed_clustering_scheduler, "Log category for Fixed Clustering Scheduler");

namespace wrench {

    FixedClusteringScheduler::FixedClusteringScheduler(
            unsigned long num_tasks_per_cluster,
            unsigned long num_nodes_per_cluster,
            unsigned long max_num_submitted_jobs) {
      if ((num_tasks_per_cluster < 1) || (num_nodes_per_cluster < 1) || (max_num_submitted_jobs < 1)) {
        throw std::invalid_argument("FixedClusteringScheduler::FixedClusteringScheduler(): invalid arguments");
      }
      this->num_tasks_per_cluster = num_tasks_per_cluster;
      this->num_nodes_per_cluster = num_nodes_per_cluster;
      this->max_num_submitted_jobs = max_num_submitted_jobs;
    }

    void FixedClusteringScheduler::scheduleTasks(const std::set<ComputeService *> &compute_services,
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
        for (unsigned long i=first_task_in_batch; i <= last_task_in_batch; i++) {
          tasks_in_job.push_back(tasks_to_schedule[i]);
          WRENCH_INFO("  - task %s (%.2lf flops)",
                      tasks_to_schedule[i]->getId().c_str(),
                      tasks_to_schedule[i]->getFlops());
        }

        // Compute the number of nodes for the job
        unsigned long num_nodes = MIN(num_tasks_in_batch, this->num_nodes_per_cluster);

        // Compute the time for the job (a bit conservative for now)
        double max_flop_best_fit_time = computeJobTime(num_nodes, tasks_in_job);

        std::map<std::string, std::string> batch_job_args;
        batch_job_args["-N"] = std::to_string(num_nodes);
        batch_job_args["-t"] = std::to_string((unsigned long)(1 + max_flop_best_fit_time / 60.0)); //time in minutes
        batch_job_args["-c"] = "1"; //number of cores per node

        StandardJob *job = this->getJobManager()->createStandardJob(tasks_in_job, {});
        WRENCH_INFO("Created a job with with batch arguments: %s:%s:%s",
            batch_job_args["-N"].c_str(),
            batch_job_args["-t"].c_str(),
            batch_job_args["-c"].c_str());

        try {
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


    double FixedClusteringScheduler::computeJobTime(
            unsigned long num_nodes,
            std::vector<WorkflowTask *> tasks) {

      // Sort the tasks by decreasing flop count
      std::sort(tasks.begin(), tasks.end(),
                [](const WorkflowTask* t1, const WorkflowTask* t2) -> bool
                {
                    if (t1->getFlops() == t2->getFlops()) {
                      return ((uintptr_t) t1 > (uintptr_t) t2);
                    } else {
                      return (t1->getFlops() > t2->getFlops());
                    }
                });

      // Print them just to check
      double completion_times[num_nodes];

      for (auto t : tasks) {
        // Find the node with the earliest completion time
        unsigned long selected_node = 0;
        for (unsigned long i = 1; i < num_nodes; i++) {
          if (completion_times[i] < completion_times[selected_node]) {
            selected_node = i;
          }
        }
        // Assign the task to it
        completion_times[selected_node] += t->getFlops();
      }

      double max_completion_time = 0;
      for (unsigned long i=0; i < num_nodes; i++) {
        if (completion_times[i] > max_completion_time) {
          max_completion_time = completion_times[i];
        }
      }
      return max_completion_time;
    }

};
