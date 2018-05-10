/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef TASK_CLUSTERING_BATCH_SIMULATOR_WORKFLOWUTIL_H
#define TASK_CLUSTERING_BATCH_SIMULATOR_WORKFLOWUTIL_H


#include <vector>

namespace wrench {

    class WorkflowTask;

    class WorkflowUtil {

    public:

        static double estimateMakespan(std::vector<WorkflowTask*> tasks, unsigned long num_hosts, double core_speed);

    };

};


#endif //TASK_CLUSTERING_BATCH_SIMULATOR_WORKFLOWUTIL_H
