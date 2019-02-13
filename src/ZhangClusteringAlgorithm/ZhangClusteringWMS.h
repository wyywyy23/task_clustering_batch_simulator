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

    class Simulator;

    class ZhangPlaceHolderJob;

    class ZhangClusteringWMS : public WMS {

    public:

        ZhangClusteringWMS(Simulator *simulator, std::string hostname, bool overlap, bool plimit,
                           BatchService *batch_service);

    private:

        BatchService *batch_service;

        int main() override;

        void applyGroupingHeuristic();

        void createAndSubmitPlaceholderJob(double requested_execution_time,
                                           unsigned long requested_parallelism,
                                           unsigned long start_level,
                                           unsigned long end_level);

        void processEventPilotJobStart(std::unique_ptr <PilotJobStartedEvent> e) override;

        void processEventPilotJobExpiration(std::unique_ptr <PilotJobExpiredEvent> e) override;

        void processEventStandardJobCompletion(std::unique_ptr <StandardJobCompletedEvent> e) override;

        void processEventStandardJobFailure(std::unique_ptr <StandardJobFailedEvent> e) override;

        double estimateWaitTime(long parallelism, double makespan, int *sequence);

        unsigned long getStartLevel();

        unsigned long maxParallelism(unsigned long start_level, unsigned long end_level);

        std::tuple<double, double, unsigned long>
        groupLevels(unsigned long start_level, unsigned long end_level, double peel_runtime[2],
                    double peel_wait_time[2]);

        Simulator *simulator;
        bool individual_mode;
        bool overlap;
        bool plimit;

        std::set<ZhangPlaceHolderJob *> running_placeholder_jobs;
        ZhangPlaceHolderJob *pending_placeholder_job;

        double core_speed;
        unsigned long number_of_hosts;

        std::shared_ptr <JobManager> job_manager;


    };

};


#endif //YOUR_PROJECT_NAME_ZHANGCLUSERINGWMS_H
