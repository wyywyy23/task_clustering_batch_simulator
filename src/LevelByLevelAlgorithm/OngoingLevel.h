/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef TASK_CLUSTERING_BATCH_SIMULATOR_ONGOINGLEVEL_H
#define TASK_CLUSTERING_BATCH_SIMULATOR_ONGOINGLEVEL_H


#include <set>
#include <Util/PlaceHolderJob.h>

namespace wrench {

    class PlaceHolderJob;

    class OngoingLevel {

    public:
        unsigned long level_number;
        std::set<PlaceHolderJob *> pending_placeholder_jobs;
        std::set<PlaceHolderJob *> running_placeholder_jobs;
        std::set<PlaceHolderJob *> completed_placeholder_jobs;

    };

};


#endif //TASK_CLUSTERING_BATCH_SIMULATOR_ONGOINGLEVEL_H
