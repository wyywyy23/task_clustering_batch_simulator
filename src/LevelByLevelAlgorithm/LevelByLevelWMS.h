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

namespace wrench {

    class PlaceHolderJob;

    class LevelByLevelWMS : public WMS {

    public:

        LevelByLevelWMS(std::string hostname, bool overlap, std::string clustering_spec, BatchService *batch_service);


    private:

        int main() override;
        void processEventPilotJobStart(std::unique_ptr<PilotJobStartedEvent> e) override;
        void processEventPilotJobExpiration(std::unique_ptr<PilotJobExpiredEvent> e) override;
        void processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> e) override;
        void processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent> e) override;

        void submitPilotJobsForNextLevel();

        std::set<PlaceHolderJob *> createPlaceHolderJobsForLevel(unsigned long level);

        bool overlap;
        std::string clustering_spec;
        BatchService *batch_service;

        std::set<PlaceHolderJob *> running_placeholder_jobs;
        std::set<PlaceHolderJob *> pending_placeholder_jobs;

        double core_speed;
        unsigned long number_of_hosts;

        std::shared_ptr<JobManager> job_manager;

        int next_level_to_submit = -1;


    };

};


#endif //TASK_CLUSTERING_BATCH_SIMULATOR_LEVELBYLEVELWMS_H
