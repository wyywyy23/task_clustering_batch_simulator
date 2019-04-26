/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef YOUR_PROJECT_NAME_EVANCLUSERINGWMS_H
#define YOUR_PROJECT_NAME_EVANCLUSERINGWMS_H


#include <wms/WMS.h>
#include <services/compute/batch/BatchService.h>


namespace wrench {

    class Simulator;

    class EvanPlaceHolderJob;

    class EvanClusteringWMS : public WMS {

    public:

        EvanClusteringWMS(Simulator *simulator, std::string hostname, bool overlap, bool plimit, double waste_bound,
                          BatchService *batch_service);

    private:

        BatchService *batch_service;

        int main() override;

        void applyGroupingHeuristic();

        void createAndSubmitPlaceholderJob(double requested_execution_time,
                                           unsigned long requested_parallelism,
                                           unsigned long start_level,
                                           unsigned long end_level);

        void processEventPilotJobStart(std::unique_ptr<PilotJobStartedEvent> e) override;

        void processEventPilotJobExpiration(std::unique_ptr<PilotJobExpiredEvent> e) override;

        void processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> e) override;

        void processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent> e) override;

        double estimateWaitTime(long parallelism, double makespan, int *sequence);

        unsigned long getStartLevel();

        unsigned long maxParallelism(unsigned long start_level, unsigned long end_level);

        std::tuple<double, double, unsigned long, unsigned long>
        groupLevels(unsigned long start_level, unsigned long end_level);

        Simulator *simulator;
        bool individual_mode;
        bool overlap;
        bool plimit;
        double waste_bound;

        std::set<EvanPlaceHolderJob *> running_placeholder_jobs;
        EvanPlaceHolderJob *pending_placeholder_job;

        double core_speed;
        unsigned long number_of_hosts;

        std::shared_ptr<JobManager> job_manager;


    };

};


#endif //YOUR_PROJECT_NAME_EVANCLUSERINGWMS_H
