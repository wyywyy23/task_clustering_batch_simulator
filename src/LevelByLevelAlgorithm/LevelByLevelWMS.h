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

    class Simulator;
    class PlaceHolderJob;
    class ClusteredJob;
    class OngoingLevel;

    class LevelByLevelWMS : public WMS {

    public:

        LevelByLevelWMS(Simulator *simulator, std::string hostname, bool overlap, std::string clustering_spec, BatchService *batch_service);


    private:

        int main() override;
        void processEventPilotJobStart(std::unique_ptr<PilotJobStartedEvent> e) override;
        void processEventPilotJobExpiration(std::unique_ptr<PilotJobExpiredEvent> e) override;
        void processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> e) override;
        void processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent> e) override;

        void submitPilotJobsForNextLevel();

        std::set<PlaceHolderJob *> createPlaceHolderJobsForLevel(unsigned long level);

//        unsigned long computeBestNumNodesBasedOnQueueWaitTimePredictions(ClusteredJob *cj);

        Simulator *simulator;

        bool overlap;
        std::string clustering_spec;
        BatchService *batch_service;


        double core_speed;
        unsigned long number_of_hosts;

        std::shared_ptr<JobManager> job_manager;

        std::map<int, OngoingLevel *> ongoing_levels;

    };

};


#endif //TASK_CLUSTERING_BATCH_SIMULATOR_LEVELBYLEVELWMS_H
