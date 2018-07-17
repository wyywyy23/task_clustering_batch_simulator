/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <Util/WorkflowUtil.h>
#include "ClusteredJob.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(clustered_job, "Log category for Clustered Job");


namespace wrench {

    void ClusteredJob::addTask(WorkflowTask *task) {
      this->tasks.push_back(task);
    }

    bool ClusteredJob::isTaskOK(wrench::WorkflowTask *task) {
      if (task->getState() == wrench::WorkflowTask::READY) {
        return true;
      }
      for (auto p : task->getWorkflow()->getTaskParents(task)) {
        if ((p->getState() != wrench::WorkflowTask::COMPLETED) &&
                (std::find(this->tasks.begin(), this->tasks.end(), p) == this->tasks.end())) {
          return false;
        }
      }
      return true;
    }

    bool ClusteredJob::isReady() {
      for (auto t : this->tasks) {
        if (not isTaskOK(t)) {
          return false;
        }
      }
      return true;
    }

    void ClusteredJob::setNumNodes(unsigned long num_nodes, bool based_on_queue_wait_time_prediction) {
      this->num_nodes = num_nodes;
      this->num_nodes_based_on_queue_wait_time_predictions = based_on_queue_wait_time_prediction;
    }

    unsigned long ClusteredJob::getNumTasks() {
      return this->tasks.size();
    }

    unsigned long ClusteredJob::getNumNodes() {
      return this->num_nodes;
    }

    std::vector<wrench::WorkflowTask *> ClusteredJob::getTasks() {
      return this->tasks;
    }

    double ClusteredJob::estimateMakespan(double core_speed) {
      if (this->num_nodes  == 0) {
        throw std::runtime_error("estimateMakespan(): Cannot estimate makespan with 0 nodes!");
      }

      return WorkflowUtil::estimateMakespan(this->tasks, this->num_nodes, core_speed);
    }


    double ClusteredJob::estimateMakespan(double core_speed, unsigned long n) {
      if (n  == 0) {
        throw std::runtime_error("estimateMakespan(): Cannot estimate makespan with 0 nodes!");
      }

      return WorkflowUtil::estimateMakespan(this->tasks, n, core_speed);
    }

    bool ClusteredJob::isNumNodesBasedOnQueueWaitTimePrediction() {
      return this->num_nodes_based_on_queue_wait_time_predictions;
    }
};