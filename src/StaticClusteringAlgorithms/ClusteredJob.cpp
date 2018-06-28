/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <WorkflowUtil/WorkflowUtil.h>
#include "ClusteredJob.h"

namespace wrench {

    void ClusteredJob::addTask(WorkflowTask *task) {
      this->tasks.push_back(task);
    }


    bool ClusteredJob::isReady() {
      for (auto t : this->tasks) {
        if (t->getState() != wrench::WorkflowTask::READY) {
          return false;
        }
      }
      return true;
    }

    void ClusteredJob::setNumNodes(unsigned long num_nodes) {
      this->num_nodes = num_nodes;
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
};