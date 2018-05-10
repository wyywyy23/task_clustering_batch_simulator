/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include <wrench-dev.h>
#include "ZhangClusteringWMS.h"
#include "PlaceHolderJob.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(zhang_clustering_wms, "Log category for Zhang Clustering WMS");

#define EXECUTION_TIME_FUDGE_FACTOR 60

namespace wrench {

    ZhangClusteringWMS::ZhangClusteringWMS(std::string hostname, BatchService *batch_service) :
            WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
      this->batch_service = batch_service;
      this->pending_placeholder_job = nullptr;
      this->individual_mode = false;
    }

    int ZhangClusteringWMS::main() {

      TerminalOutput::setThisProcessLoggingColor(COLOR_WHITE);

      // Find out core speed on the batch service
      this->core_speed = *(this->batch_service->getCoreFlopRate().begin());
      // Find out #hosts on the batch service
      this->num_hosts = this->batch_service->getNumHosts();

      // Create a job manager
      this->job_manager = this->createJobManager();

      while (not workflow->isDone()) {

        // Submit a pilot job (if needed)
        applyGroupingHeuristic();

        this->waitForAndProcessNextEvent();

      }

      return 0;
    }



    /**
     *
     */
    void ZhangClusteringWMS::applyGroupingHeuristic() {

      // Don't schedule a pilot job if one is pending
      if (this->pending_placeholder_job) {
        return;
      }

      // Dont schedule a pilot job if we're in individual mode
      if (this->individual_mode) {
        return;
      }

      // Compute my start level as the next "not started level"
      unsigned long start_level = 0;
      for (auto ph : this->running_placeholder_jobs) {
        start_level =  1 + MAX(start_level, ph->end_level);
      }

      // Nothing to do?
      if (start_level >= this->workflow->getNumLevels()) {
        return;
      }

      double best_ratio = DBL_MAX;
      unsigned long end_level;
      unsigned long requested_parallelism;
      double estimated_wait_time;
      double requested_execution_time;

      // Apply the DAG GROUPING heuristic (Fig. 5 in the paper)
      for (unsigned long candidate_end_level = start_level; candidate_end_level < this->workflow->getNumLevels(); candidate_end_level++) {
        std::tuple<double, double, unsigned long> wait_run_par = computeLevelGroupingRatio(start_level, candidate_end_level);
        double wait_time = std::get<0>(wait_run_par);
        double run_time = std::get<1>(wait_run_par);
        unsigned long parallelism = std::get<2>(wait_run_par);
        double ratio = wait_time / run_time;
        if (ratio <= best_ratio) {
          end_level = candidate_end_level;
          best_ratio = ratio;
          requested_execution_time = run_time;
          requested_parallelism = parallelism;
          estimated_wait_time = wait_time;
        } else {
          break;
        }
      }

      this->individual_mode = (end_level == this->workflow->getNumLevels() -1) and
                              (requested_execution_time * 2.0 >= estimated_wait_time);

      if (not individual_mode) {
        createAndSubmitPlaceholderJob(
                requested_execution_time,
                requested_parallelism,
                start_level,
                end_level);
      } else {
        WRENCH_INFO("Switching to individual mode!");
        // Submit all READY tasks as individual jobs
        for (auto task : this->workflow->getTasks()) {
          if (task->getState() == WorkflowTask::State::READY) {
            StandardJob *standard_job = this->job_manager->createStandardJob(task,{});
            std::map<std::string, std::string> service_specific_args;
            requested_execution_time = task->getFlops() / this->core_speed + EXECUTION_TIME_FUDGE_FACTOR;
            service_specific_args["-N"] = "1";
            service_specific_args["-c"] = "1";
            service_specific_args["-t"] = std::to_string(1 + ((unsigned long) requested_execution_time) / 60);
            WRENCH_INFO("Submitting task %s individually!", task->getId().c_str());
            this->job_manager->submitJob(standard_job, this->batch_service, service_specific_args);
          }
        }
      }

    }


    /**
     *
     * @param requested_execution_time
     * @param requested_parallelism
     * @param start_level
     * @param end_level
     */
    void ZhangClusteringWMS::createAndSubmitPlaceholderJob(
            double requested_execution_time,
            unsigned long requested_parallelism,
            unsigned long start_level,
            unsigned long end_level) {

      requested_execution_time = requested_execution_time + EXECUTION_TIME_FUDGE_FACTOR;

      // Aggregate tasks
      std::vector<WorkflowTask *> tasks;
      for (unsigned long l = start_level; l <= end_level; l++) {
        std::vector<WorkflowTask *> tasks_in_level = this->workflow->getTasksInTopLevelRange(l, l);
        for (auto t : tasks_in_level) {
          if (t->getState() != WorkflowTask::COMPLETED) {
            tasks.push_back(t);
          }
        }
      }

      // Submit the pilot job
      std::map<std::string, std::string> service_specific_args;
      service_specific_args["-N"] = std::to_string(requested_parallelism);
      service_specific_args["-c"] = "1";
      service_specific_args["-t"] = std::to_string(1 + ((unsigned long) requested_execution_time) / 60);


      // Keep track of the placeholder job
      this->pending_placeholder_job = new PlaceHolderJob(
              this->job_manager->createPilotJob(requested_parallelism, 1, 0.0, requested_execution_time),
              tasks,
              start_level,
              end_level);

      WRENCH_INFO("Submitting a Pilot Job (%ld hosts, %.2lf sec) for workflow levels %ld-%ld (%s)",
                  requested_parallelism, requested_execution_time, start_level, end_level,
                  this->pending_placeholder_job->pilot_job->getName().c_str());
      WRENCH_INFO("This pilot job has these tasks:");
      for (auto t : this->pending_placeholder_job->tasks) {
        WRENCH_INFO("     - %s", t->getId().c_str());
      }

      // submit the corresponding pilot job
      this->job_manager->submitJob(this->pending_placeholder_job->pilot_job, this->batch_service,
                                   service_specific_args);
    }


    void ZhangClusteringWMS::processEventPilotJobStart(std::unique_ptr<PilotJobStartedEvent> e) {
      // Just for kicks, check it was the pending one
      WRENCH_INFO("Got a Pilot Job Start event: %s", e->pilot_job->getName().c_str());
      if (this->pending_placeholder_job == nullptr) {
        WRENCH_INFO("FATAL!!! Got a PILOT JOB START EVENT, but PENDING_PLACEHOLDER == NULLPTR");
        exit(1);
      }
//      WRENCH_INFO("Got a Pilot Job Start event e->pilot_job = %ld, this->pending->pilot_job = %ld (%s)",
//                  (unsigned long) e->pilot_job,
//                  (unsigned long) this->pending_placeholder_job->pilot_job,
//                  this->pending_placeholder_job->pilot_job->getName().c_str());

      if (e->pilot_job != this->pending_placeholder_job->pilot_job) {

        WRENCH_INFO("Must be for a placeholder I already cancelled... nevermind");
        return;
      }

      PlaceHolderJob *placeholder_job = this->pending_placeholder_job;

      // Move it to running
      this->running_placeholder_jobs.insert(placeholder_job);
      this->pending_placeholder_job = nullptr;

      // Submit all ready tasks to it each in its standard job
      std::string output_string = "";
      for (auto task : placeholder_job->tasks) {
        if (task->getState() == WorkflowTask::READY) {
          StandardJob *standard_job = this->job_manager->createStandardJob(task,{});
          output_string += " " + task->getId();

          WRENCH_INFO("Submitting task %s as part of placeholder job %ld-%ld",
                      task->getId().c_str(), placeholder_job->start_level, placeholder_job->end_level);
          this->job_manager->submitJob(standard_job, placeholder_job->pilot_job->getComputeService());
        }
      }

      // Re-submit a pilot job
      this->applyGroupingHeuristic();

    }

    void ZhangClusteringWMS::processEventPilotJobExpiration(std::unique_ptr<PilotJobExpiredEvent> e) {

      // Find the placeholder job
      PlaceHolderJob *placeholder_job = nullptr;
      for (auto ph : this->running_placeholder_jobs) {
        if (ph->pilot_job == e->pilot_job) {
          placeholder_job = ph;
          break;
        }
      }
      if (placeholder_job == nullptr) {
        throw std::runtime_error("Got a pilot job expiration, but no matching placeholder job found");
      }

      WRENCH_INFO("Got a pilot job expiration for a placeholder job that deals with levels %ld-%ld (%s)",
                  placeholder_job->start_level, placeholder_job->end_level, placeholder_job->pilot_job->getName().c_str());
      // Check if there are unprocessed tasks
      bool unprocessed = (placeholder_job->tasks.size() != placeholder_job->num_completed_tasks);

      if (not unprocessed) { // Nothing to do
        WRENCH_INFO("This placeholder job has no unprocessed tasks. great.");
        return;
      }

      WRENCH_INFO("This placeholder job has unprocessed tasks");

      // Cancel pending pilot job if any
      if (this->pending_placeholder_job) {
        WRENCH_INFO("Canceling pending placeholder job (placeholder=%ld,  pilot_job=%ld / %s",
                    (unsigned long)this->pending_placeholder_job,
                    (unsigned long)this->pending_placeholder_job->pilot_job,
                    this->pending_placeholder_job->pilot_job->getName().c_str());
        this->job_manager->terminateJob(this->pending_placeholder_job->pilot_job);
        this->pending_placeholder_job = nullptr;
      }

      // Cancel running pilot jobs if none of their tasks has started

      std::set<PlaceHolderJob *> to_remove;
      for (auto ph : this->running_placeholder_jobs) {
        bool started = false;
        for (auto task : ph->tasks) {
          if (task->getState() != WorkflowTask::NOT_READY) {
            started = true;
          }
        }
        if (not started) {
          WRENCH_INFO("Canceling running placeholder job that handled levels %ld-%ld because none"
                              "of its tasks has started (%s)", ph->start_level, ph->end_level,
                      ph->pilot_job->getName().c_str());
          try {
            this->job_manager->terminateJob(ph->pilot_job);
          } catch (WorkflowExecutionException &e) {
            // ignore (likely already dead!)
          }
          to_remove.insert(ph);
        }
      }

      for (auto ph : to_remove) {
        this->running_placeholder_jobs.erase(ph);
      }

      // Make decisions again
      applyGroupingHeuristic();

    }

    void ZhangClusteringWMS::processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> e) {

      WorkflowTask *completed_task = e->standard_job->tasks[0]; // only one task per job

      WRENCH_INFO("Got a standard job completion for task %s", completed_task->getId().c_str());

      // Find the placeholder job this task belongs to
      PlaceHolderJob *placeholder_job = nullptr;
      for (auto ph : this->running_placeholder_jobs) {
        for (auto task : ph->tasks) {
          if (task == completed_task) {
            placeholder_job = ph;
            break;
          }
        }
      }

      if ((placeholder_job == nullptr) and (not this->individual_mode)) {
        throw std::runtime_error("Got a task completion, but couldn't find a placeholder for the task, "
                                         "and we're not in individual mode");
      }

      if (placeholder_job != nullptr) {

        placeholder_job->num_completed_tasks++;
        // Terminate the standard job in case all its tasks are done
        if (placeholder_job->num_completed_tasks == placeholder_job->tasks.size()) {
          WRENCH_INFO("All tasks are completed in this placeholder job, so I am terminating it (%s)",
                      placeholder_job->pilot_job->getName().c_str());
          try {
            this->job_manager->terminateJob(placeholder_job->pilot_job);
          } catch (WorkflowExecutionException &e) {
            // ignore
          }
          this->running_placeholder_jobs.erase(placeholder_job);
        }

      }

      // Start all newly ready tasks that depended on the completed task, IN ANY PLACEHOLDER
      // This shouldn't happen in individual mode, but can't hurt
      std::vector<WorkflowTask *>children = this->workflow->getTaskChildren(completed_task);
      for (auto ph : this->running_placeholder_jobs) {
        for (auto task : ph->tasks) {
          if ((std::find(children.begin(), children.end(), task) != children.end()) and
              (task->getState() == WorkflowTask::READY)) {
            StandardJob *standard_job = this->job_manager->createStandardJob(task,{});
            WRENCH_INFO("Submitting task %s  as part of placeholder job %ld-%ld",
                        task->getId().c_str(), placeholder_job->start_level, placeholder_job->end_level);
            this->job_manager->submitJob(standard_job, ph->pilot_job->getComputeService());
          }
        }
      }

      if (this->individual_mode) {
        for (auto task : this->workflow->getTasks()) {
          if (task->getState() == WorkflowTask::State::READY) {
            StandardJob *standard_job = this->job_manager->createStandardJob(task,{});
            WRENCH_INFO("Submitting task %s individually!",
                        task->getId().c_str());
            std::map<std::string, std::string> service_specific_args;
            double requested_execution_time = task->getFlops() / this->core_speed + EXECUTION_TIME_FUDGE_FACTOR;
            service_specific_args["-N"] = "1";
            service_specific_args["-c"] = "1";
            service_specific_args["-t"] = std::to_string(1 + ((unsigned long) requested_execution_time) / 60);
            this->job_manager->submitJob(standard_job, this->batch_service, service_specific_args);
          }
        }
      }



    }

    void ZhangClusteringWMS::processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent> e) {
//      WRENCH_INFO("Got a standard job failure event for task %s -- IGNORING THIS", e->standard_job->tasks[0]->getId().c_str());
    }



    /**
     *
     * @param start_level
     * @param end_level
     * @return
     */
    std::tuple<double, double, unsigned long> ZhangClusteringWMS::computeLevelGroupingRatio(
            unsigned long start_level, unsigned long end_level) {

      // Figure out parallelism
      unsigned long parallelism = 0;
      for (unsigned long l = start_level; l <= end_level; l++) {
        unsigned long num_tasks_in_level = this->workflow->getTasksInTopLevelRange(l,l).size();
        if (num_tasks_in_level > this->num_hosts) {
          throw std::runtime_error("ZhangClusteringWMS::applyGroupingHeuristic(): Workflow level " +
                                   std::to_string(l) +
                                   " has more tasks than" +
                                   "number of hosts on the batch service, which is not" +
                                   "handled by the algorithm by Zhang et al.");
        }
        parallelism = MAX(parallelism, num_tasks_in_level);
      }

      // Figure out the maximum execution time
      double execution_time = 0;
      for (unsigned long l = start_level; l <= end_level; l++) {
        double max_exec_time_in_level = 0;
        std::vector<WorkflowTask *> tasks_in_level = this->workflow->getTasksInTopLevelRange(l,l);
        for (auto t : tasks_in_level) {
          max_exec_time_in_level = MAX(max_exec_time_in_level,  t->getFlops() / core_speed);
        }
        execution_time += max_exec_time_in_level;
      }

      // Figure out the estimated wait time
//      std::map<std::string,double> getQueueWaitingTimeEstimate(std::set<std::tuple<std::string,unsigned int,unsigned int, double>>);
      std::set<std::tuple<std::string,unsigned int,unsigned int, double>> job_config;
      job_config.insert(std::make_tuple("config", (unsigned int)parallelism, 1, execution_time));
      std::map<std::string, double> estimates = this->batch_service->getQueueWaitingTimeEstimate(job_config);
      double wait_time_estimate = estimates["config"];

      WRENCH_INFO("GroupLevel(%ld,%ld): parallelism=%ld, wait_time=%.2lf, execution_time=%.2lf",
                  start_level, end_level, parallelism, wait_time_estimate, execution_time);

      return std::make_tuple(wait_time_estimate, execution_time, parallelism);
    }



};