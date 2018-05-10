/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef YOUR_PROJECT_NAME_ZHANGCLUSERINGWMS_H
#define YOUR_PROJECT_NAME_ZHANGCLUSERINGWMS_H


#include <wms/WMS.h>
#include <services/compute/batch/BatchService.h>


namespace wrench {

    class PlaceHolderJob;

    class ZhangClusteringWMS : public WMS {

    public:

        ZhangClusteringWMS(std::string hostname, BatchService *batch_service);

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
        std::tuple<double, double, unsigned long> computeLevelGroupingRatio(unsigned long start_level, unsigned long end_level);

        bool individual_mode;

        std::set<PlaceHolderJob *> running_placeholder_jobs;
        PlaceHolderJob *pending_placeholder_job;

        double core_speed;
        unsigned long number_of_hosts;

        std::shared_ptr<JobManager> job_manager;


    };

};


#endif //YOUR_PROJECT_NAME_ZHANGCLUSERINGWMS_H
