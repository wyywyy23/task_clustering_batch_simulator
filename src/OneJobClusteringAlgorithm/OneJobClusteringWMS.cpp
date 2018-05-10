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

      return 6000;
      return 0;
    }


};