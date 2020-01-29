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


#include <services/compute/batch/BatchComputeService.h>

namespace wrench {

    class Simulator;
    class PlaceHolderJob;
    class ClusteredJob;
    class OngoingLevel;

    class LevelByLevelWMS : public WMS {

    public:

        LevelByLevelWMS(Simulator *simulator, std::string hostname, bool overlap,
                std::string clustering_spec, std::shared_ptr<BatchComputeService> batch_service);


    private:

        int main() override;
        void processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent> e) override;
        void processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent> e) override;
        void processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> e) override;
        void processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> e) override;

        void submitPilotJobsForNextLevel();

        std::set<PlaceHolderJob *> createPlaceHolderJobsForLevel(unsigned long level);

//        unsigned long computeBestNumNodesBasedOnQueueWaitTimePredictions(ClusteredJob *cj);

        Simulator *simulator;

        bool overlap;
        std::string clustering_spec;
        std::shared_ptr<BatchComputeService> batch_service;


        double core_speed;
        unsigned long number_of_nodes;

        std::shared_ptr<JobManager> job_manager;

        std::map<int, OngoingLevel *> ongoing_levels;

        unsigned long last_level_completed = ULONG_MAX;
    };

};


#endif //TASK_CLUSTERING_BATCH_SIMULATOR_LEVELBYLEVELWMS_H
