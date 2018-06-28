/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef TASK_CLUSTERING_BATCH_SIMULATOR_CLUSTEREDJOB_H
#define TASK_CLUSTERING_BATCH_SIMULATOR_CLUSTEREDJOB_H

#include <wrench-dev.h>

namespace wrench {

    class ClusteredJob {

    public:
        void setNumNodes(unsigned long num_nodes);
        unsigned long getNumNodes();
        unsigned long getNumTasks();
        std::vector<wrench::WorkflowTask *>  getTasks();
        void addTask(wrench::WorkflowTask *task);
        bool isReady();
        double estimateMakespan(double core_speed);

    private:
        std::vector<wrench::WorkflowTask *> tasks;
        unsigned long num_nodes = 0;
    };

};


#endif //TASK_CLUSTERING_BATCH_SIMULATOR_CLUSTEREDJOB_H
