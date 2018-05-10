/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include <wrench-dev.h>
#include <workflow/execution_events/WorkflowExecutionEvent.h>
#include "OneJobClusteringWMS.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(one_job_clustering_wms, "Log category for One Job Clustering WMS");

#define EXECUTION_TIME_FUDGE_FACTOR 60

namespace wrench {

    OneJobClusteringWMS::OneJobClusteringWMS(std::string hostname, BatchService *batch_service) :
            WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
      this->batch_service = batch_service;
    }

    int OneJobClusteringWMS::main() {

      TerminalOutput::setThisProcessLoggingColor(COLOR_WHITE);

      // Find out core speed on the batch service
      this->core_speed = *(this->batch_service->getCoreFlopRate().begin());
      // Find out #hosts on the batch service
      this->num_hosts = this->batch_service->getNumHosts();

      // Create a job manager
      this->job_manager = this->createJobManager();

      submitSingleJob();

      this->waitForAndProcessNextEvent();

      if (not this->workflow->isDone()) {
        throw std::runtime_error("OneJobClusteringWMS::main(): The workflow should be done!");
      }


      return 0;
    }



    void OneJobClusteringWMS::submitSingleJob() {

      // Compute parallelism
      unsigned long parallelism = 0;
      for (int i=0; i < this->workflow->getNumLevels(); i++) {
        parallelism = MAX(parallelism, this->workflow->getTasksInTopLevelRange(i,i).size());
      }

      // Find the best job configuration
      unsigned long picked_num_hosts;
      double picked_makespan;
      double best_totaltime = DBL_MAX;
      WRENCH_INFO("Choosing best configuration:");
      for (unsigned long num_hosts = 1; num_hosts <= parallelism; num_hosts++) {
        double makespan = computeWorkflowMakespan(num_hosts);
        std::set<std::tuple<std::string,unsigned int,unsigned int, double>> job_config;
        job_config.insert(std::make_tuple("config", (unsigned int)parallelism, 1, makespan));
        std::map<std::string, double> estimates = this->batch_service->getQueueWaitingTimeEstimate(job_config);
        double waittime = estimates["config"];

        WRENCH_INFO(" - With %ld hosts, wait time + makespan = %.2lf + %.2lf = %.2lf",
                    num_hosts, waittime, makespan, waittime + makespan);

        if (waittime + makespan  < best_totaltime) {
          picked_num_hosts = num_hosts;
          picked_makespan = makespan;
          best_totaltime = waittime + makespan;
        }
      }

      // Create job
      StandardJob *job = this->job_manager->createStandardJob(this->workflow->getTasks(), {});

      // Submit DAG in Job
      std::map<std::string, std::string> service_specific_args;
      double requested_time = picked_makespan + EXECUTION_TIME_FUDGE_FACTOR;
      service_specific_args["-N"] = std::to_string(picked_num_hosts);
      service_specific_args["-c"] = "1";
      service_specific_args["-t"] = std::to_string(1 + ((unsigned long) requested_time) / 60);

      WRENCH_INFO("Submitting the workflow in a single %ld-host job", picked_num_hosts);
      this->job_manager->submitJob(job, this->batch_service, service_specific_args);

    }

    /**
     *
     * @param e
     */
    void OneJobClusteringWMS::processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> e) {

      WRENCH_INFO("Received a standard job completion");
    }

    /**
     *
     * @param num_hosts
     * @return
     */
    double OneJobClusteringWMS::computeWorkflowMakespan(unsigned long num_hosts) {

      // Initialize host idle dates
      double idle_date[num_hosts];
      for (int i=0; i < num_hosts; i++) {
        idle_date[i] = 0.0;
      }


      // Create a list of "fake" fake_tasks
      std::tuple<WorkflowTask *, double> fake_tasks[this->workflow->getNumberOfTasks()];  // WorkflowTask, completion time

      // Insert all fake_tasks
      int i=0;
      for (auto task : this->workflow->getTasks()) {
        std::tuple<WorkflowTask *, double> fake_task;
        fake_task = std::make_tuple(task, -1.0);
        fake_tasks[i++] = fake_task;
      }


      unsigned long num_scheduled_tasks = 0;
      double current_time = 0.0;

      while (num_scheduled_tasks < this->workflow->getNumberOfTasks()) {

        bool scheduled_something = false;
        // Schedule ALL READY Tasks
        for (i=0; i <  this->workflow->getNumberOfTasks(); i++)  {

          auto ft = fake_tasks[i];

          if (std::get<1>(ft) >= 0.0) {
            continue;
          }

          WorkflowTask *real_task = std::get<0>(ft);

//          WRENCH_INFO("LOOKING AT TASK %s", real_task->getId().c_str());

          // Determine whether the task is schedulable
          bool schedulable = true;
          for (auto parent : real_task->getWorkflow()->getTaskParents(real_task)) {
            for (int k=0; k < this->workflow->getNumberOfTasks(); k++) {
              if (std::get<0>(fake_tasks[k]) == parent) {
//                WRENCH_INFO("    LOOKING AT PARENT %s:  %.2lf", parent->getId().c_str(), std::get<1>(fake_tasks[k]));
                if ((std::get<1>(fake_tasks[k]) > current_time) or
                    (std::get<1>(fake_tasks[k]) < 0)) {
                  schedulable = false;
                  break;
                }
              }
              if (not schedulable) {
                break;
              }
            }
          }

          if (not schedulable) {
//            WRENCH_INFO("NOT SCHEDULABLE");
            continue;
          }

          for (int j=0; j < num_hosts; j++) {
//            WRENCH_INFO("LOOKING AT HOST %d: %.2lf", j, idle_date[j]);
            if (idle_date[j] <= current_time) {
//              WRENCH_INFO("SCHEDULING TASK on HOST %d", j);
              double new_time = current_time + real_task->getFlops() / this->core_speed;
              fake_tasks[i] = std::make_tuple(std::get<0>(ft), new_time);

//              for (int k=0; k < this->workflow->getNumberOfTasks(); k++) {
//                WRENCH_INFO("------> %.2lf", std::get<1>(fake_tasks[k]));
//              }

              idle_date[j] = current_time + real_task->getFlops() / this->core_speed;
//              WRENCH_INFO("SCHEDULED TASK %s on host %d from time %.2lf-%.2lf",
//                          real_task->getId().c_str(), j, current_time,
//                          current_time + real_task->getFlops() / this->core_speed);
              scheduled_something = true;
              num_scheduled_tasks++;
              break;
            } else {
//              WRENCH_INFO("THIS HOST DOESN'T WORK");
            }
          }
        }
//        WRENCH_INFO("UPDATING CURRENT TIME");
        if (scheduled_something) {
          // Set current time to min idle time
          double min_idle_time = DBL_MAX;
          for (int j = 0; j < num_hosts; j++) {
            if (idle_date[j] < min_idle_time) {
              min_idle_time = idle_date[j];
            }
          }
          current_time = min_idle_time;
        } else {
          // Set current time to next-to-min idle time
          double min_idle_time = DBL_MAX;
          for (int j = 0; j < num_hosts; j++) {
            if (idle_date[j] < min_idle_time) {
              min_idle_time = idle_date[j];
            }
          }
          double second_min_idle_time = DBL_MAX;
          for (int j = 0; j < num_hosts; j++) {
            if ((idle_date[j] > min_idle_time) and (idle_date[j] < second_min_idle_time)) {
              second_min_idle_time = idle_date[j];
            }
          }

          current_time = second_min_idle_time;
        }
//        WRENCH_INFO("UPDATED CURRENT TIME TO %.2lf", current_time);
      }

      double makespan = 0;
      for (int i=0; i < num_hosts; i++) {
        makespan = MAX(makespan, idle_date[i]);
      }
      return makespan;

    }


};