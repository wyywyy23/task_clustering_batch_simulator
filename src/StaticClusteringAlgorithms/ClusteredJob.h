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
#include "Simulator.h"

namespace wrench {

    class ClusteredJob {

    public:
        void setNumNodes(unsigned long num_nodes, bool based_on_queue_wait_time_prediction = false);

        unsigned long getNumNodes();

        unsigned long getNumTasks();

        std::vector<wrench::WorkflowTask *> getTasks();

        void addTask(wrench::WorkflowTask *task);

        bool isReady();

        bool isTaskOK(wrench::WorkflowTask *task);

        double estimateMakespan(double core_speed);

        double estimateMakespan(double core_speed, unsigned long num_nodes);

        unsigned long getMaxParallelism();

        unsigned long computeBestNumNodesBasedOnQueueWaitTimePredictions(unsigned long max_num_nodes, double core_speed,
                                                                         std::shared_ptr<BatchComputeService> batch_service);

        bool isNumNodesBasedOnQueueWaitTimePrediction();

        void setWasteBound(double waste_bound);

    private:
        std::vector<wrench::WorkflowTask *> tasks;
        unsigned long num_nodes = 0;
        bool num_nodes_based_on_queue_wait_time_predictions = false;
        double waste_bound = 1;
    };

};


#endif //TASK_CLUSTERING_BATCH_SIMULATOR_CLUSTEREDJOB_H
