/**
 * Copyright (c) 2019. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef YOUR_PROJECT_NAME_GLUMEWMS_H
#define YOUR_PROJECT_NAME_GLUMEWMS_H


#include <wrench-dev.h>
#include "Simulator.h"
#include <Util/PlaceHolderJob.h>
#include <Util/ProxyWMS.h>

namespace wrench {

    class GlumeWMS : public WMS {

    public:

        GlumeWMS(Simulator *simulator, std::string hostname, double waste_bound, double beat_bound,
                          std::shared_ptr<BatchComputeService> batch_service);

    private:

        std::shared_ptr<BatchComputeService> batch_service;

        int main() override;

        void applyGroupingHeuristic();

        std::tuple<double, double, unsigned long>
        estimateJob(unsigned long start_level, unsigned long end_level, double delay);

        unsigned long findMaxParallelism(unsigned long start_level, unsigned long end_level);

        std::tuple<double, double>
        estimateWaitAndRunTimes(unsigned long start_level, unsigned long end_level, unsigned long nodes);

        bool isTooWasteful(double runtime, unsigned long nodes, unsigned long start_level,
                                              unsigned long end_level);

        double calculateLeewayBinarySearch(double runtime, unsigned long num_nodes, double parent_runtime, double lower,
                                           double upper);

        void processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent> e) override;

        void processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent> e) override;

        void processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> e) override;

        void processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> e) override;

        Simulator *simulator;

        double waste_bound;
        double beat_bound;

        std::set<PlaceHolderJob *> running_placeholder_jobs;
        PlaceHolderJob *pending_placeholder_job;
        double core_speed;
        unsigned long number_of_hosts;
        std::shared_ptr<JobManager> job_manager;

        ProxyWMS *proxyWMS;

        unsigned long number_of_splits;
    };

};


#endif //YOUR_PROJECT_NAME_GLUMEWMS_H
