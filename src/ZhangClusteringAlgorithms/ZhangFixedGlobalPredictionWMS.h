/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef YOUR_PROJECT_NAME_ZHANGFIXEDGLOBALPREDICTIONWMS_H
#define YOUR_PROJECT_NAME_ZHANGFIXEDGLOBALPREDICTIONWMS_H


#include <wms/WMS.h>
#include <services/compute/batch/BatchComputeService.h>


namespace wrench {

    class Simulator;

    class PlaceHolderJob;

    class ProxyWMS;

    class ZhangFixedGlobalPredictionWMS : public WMS {

    public:

        ZhangFixedGlobalPredictionWMS(Simulator *simulator, std::string hostname, std::shared_ptr<BatchComputeService> batch_service);

    private:

        std::shared_ptr<BatchComputeService> batch_service;

        int main() override;

        void applyGroupingHeuristic();

        void processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent> e) override;

        void processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent> e) override;

        void processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> e) override;

        void processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> e) override;

        unsigned long getStartLevel();

        unsigned long maxParallelism(unsigned long start_level, unsigned long end_level);

        unsigned long bestParallelism(unsigned long start_level, unsigned long end_level);

        std::tuple<double, double, unsigned long, unsigned long> groupLevels(unsigned long start_level, unsigned long end_level);

        Simulator *simulator;
        bool individual_mode;

        std::set<PlaceHolderJob *> running_placeholder_jobs;
        PlaceHolderJob *pending_placeholder_job;

        double core_speed;
        unsigned long number_of_hosts;

        std::shared_ptr<JobManager> job_manager;

        unsigned long number_of_splits = 0; // Number of times the workflow was split

        ProxyWMS *proxyWMS;

    };

};


#endif //YOUR_PROJECT_NAME_ZHANGFIXEDGLOBALWMS_H
