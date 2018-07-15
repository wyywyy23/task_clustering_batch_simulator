/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef TASK_CLUSTERING_BATCH_SIMULATOR_LEVELBYLEVELWMS_H
#define TASK_CLUSTERING_BATCH_SIMULATOR_LEVELBYLEVELWMS_H


#include <services/compute/batch/BatchService.h>
#include <Util/PlaceHolderJob.h>

namespace wrench {

    class LevelByLevelWMS : public WMS {

    public:

        LevelByLevelWMS(std::string hostname, bool overlap, std::string clustering_spec, BatchService *batch_service);

    private:
        int main() override;


        bool overlap;
        std::string clustering_spec;
        BatchService *batch_service;

        std::set<PlaceHolderJob *> running_placeholder_jobs;
        PlaceHolderJob *pending_placeholder_job;

        double core_speed;
        unsigned long number_of_hosts;

        std::shared_ptr<JobManager> job_manager;


    };

};


#endif //TASK_CLUSTERING_BATCH_SIMULATOR_LEVELBYLEVELWMS_H
