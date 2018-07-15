/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include <wms/WMS.h>
#include <workflow/job/PilotJob.h>
#include <logging/TerminalOutput.h>
#include <Util/PlaceHolderJob.h>
#include <managers/JobManager.h>
#include <StaticClusteringAlgorithms/ClusteredJob.h>
#include <StaticClusteringAlgorithms/StaticClusteringWMS.h>
#include "LevelByLevelWMS.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(level_by_level_clustering_wms, "Log category for Level-by-Level Clustering WMS");


#define EXECUTION_TIME_FUDGE_FACTOR 1.1

namespace wrench {

    LevelByLevelWMS::LevelByLevelWMS(std::string hostname, bool overlap, std::string clustering_spec,
                                     BatchService *batch_service) :
            WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
      this->overlap = overlap;
      this->batch_service = batch_service;
      this->clustering_spec = clustering_spec;
    }


    int LevelByLevelWMS::main() {

      TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_WHITE);

      this->checkDeferredStart();

      // Find out core speed on the batch service
      this->core_speed = *(this->batch_service->getCoreFlopRate().begin());
      // Find out #hosts on the batch service
      this->number_of_hosts = this->batch_service->getNumHosts();

      // Create a job manager
      this->job_manager = this->createJobManager();

      while (not this->getWorkflow()->isDone()) {

        // Submit a pilot job (if needed)
        submitPilotJobsForNextLevel();

        this->waitForAndProcessNextEvent();

      }

      return 0;
    }


    void LevelByLevelWMS::submitPilotJobsForNextLevel() {

      // Don't schedule a pilot job if one is pending
      if (not this->pending_placeholder_jobs.empty()) {
        return;
      }

      // Don't schedule a pilot job if overlap = false and pilot job is running
      if ((not this->overlap) and (not this->running_placeholder_jobs.empty())) {
        return;
      }

      // At this point, we should submit pilot jobs for the next level
      next_level_to_submit++;

      WRENCH_INFO("Submitting pilot jobs for level %d", next_level_to_submit);

      // Create all pilot jobs for level
      std::set<PlaceHolderJob *> place_holder_jobs;
      place_holder_jobs = createPlaceHolderJobsForLevel(next_level_to_submit);

      // Submit placeholder jobs
      for (auto ph : place_holder_jobs) {
        this->pending_placeholder_jobs.insert(ph);
        // submit the corresponding pilot job
        std::map<std::string, std::string> service_specific_args;
        service_specific_args["-N"] = std::to_string(ph->pilot_job->getNumHosts());
        service_specific_args["-c"] = std::to_string(ph->pilot_job->getNumCoresPerHost());
        service_specific_args["-t"] = std::to_string(1 + ((unsigned long) ph->pilot_job->getDuration()) / 60);
        this->job_manager->submitJob(ph->pilot_job, this->batch_service,
                                     service_specific_args);
        WRENCH_INFO("Submitted a Pilot Job (%s hosts, %s min) for workflow level %d (%s)",
                    service_specific_args["-N"].c_str(),
                    service_specific_args["-t"].c_str(),
                    next_level_to_submit,
                    ph->pilot_job->getName().c_str());
        WRENCH_INFO("This pilot job has these tasks:");
        for (auto t : ph->tasks) {
          WRENCH_INFO("     - %s", t->getID().c_str());
        }
      }
    }

    std::set<PlaceHolderJob *> LevelByLevelWMS::createPlaceHolderJobsForLevel(unsigned long level) {

      // Get tasks
      std::vector<WorkflowTask *> tasks;

      std::vector<WorkflowTask *> tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(level,level);

      for (auto t : tasks_in_level) {
        if (t->getState() != WorkflowTask::COMPLETED) {
          tasks.push_back(t);
        }
      }

      // Create clustered jobs based on heuristics
      std::istringstream ss(this->clustering_spec);
      std::string token;
      std::vector<std::string> tokens;

      while(std::getline(ss, token, '-')) {
        tokens.push_back(token);
      }

      std::set<ClusteredJob *>clustered_jobs;
      if (tokens[0] == "hc") {
        if (tokens.size() != 3) {
          throw std::runtime_error("createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
        }
        unsigned long num_tasks_per_cluster;
        unsigned long num_nodes_per_cluster;
        if ((sscanf(tokens[2].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
            (sscanf(tokens[3].c_str(), "%lu", &num_nodes_per_cluster) != 1) or (num_nodes_per_cluster < 1)) {
          throw std::invalid_argument("Invalid static:hc specification");
        }
        clustered_jobs = StaticClusteringWMS::createHCJobs(
                "none", num_tasks_per_cluster, num_nodes_per_cluster,
                this->getWorkflow(), level, level);

      } else {
        throw std::runtime_error("createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
      }


    }


};