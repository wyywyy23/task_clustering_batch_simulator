/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef YOUR_PROJECT_NAME_TESTCLUSERINGWMS_H
#define YOUR_PROJECT_NAME_TESTCLUSERINGWMS_H


#include <wms/WMS.h>
#include <services/compute/batch/BatchComputeService.h>


namespace wrench {

    class Simulator;

    class TestPlaceHolderJob;

    class TestClusteringWMS : public WMS {

    public:

        TestClusteringWMS(Simulator *simulator, std::string hostname, bool overlap, bool plimit, double waste_bound,
                          double beat_bound, std::shared_ptr<BatchComputeService> batch_service);

    private:

        std::shared_ptr<BatchComputeService> batch_service;

        int main() override;

        void applyGroupingHeuristic();

        void createAndSubmitPlaceholderJob(double requested_execution_time,
                                           unsigned long requested_parallelism,
                                           unsigned long start_level,
                                           unsigned long end_level);

        void processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent> e) override;

        void processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent> e) override;

        void processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> e) override;

        void processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> e) override;

        double
        estimateWaitTime(long parallelism, double makespan, int *sequence);

        std::tuple<double, double, unsigned long>
        computeBestNumHosts(unsigned long start_level, unsigned long end_level);

        std::tuple<double, double> estimateTotalTime(
                unsigned long start_level, unsigned long end_level, unsigned long num_hosts);

        unsigned long findMaxTasks(unsigned long start_level, unsigned long end_level);

        Simulator *simulator;
        bool individual_mode;
        bool overlap;
        bool plimit;
        double waste_bound;
        double beat_bound;

        std::set<TestPlaceHolderJob *> running_placeholder_jobs;
        TestPlaceHolderJob *pending_placeholder_job;

        double core_speed;
        unsigned long number_of_hosts;

        std::shared_ptr<JobManager> job_manager;

        unsigned long number_of_splits = 0; // Number of times the workflow was split


    };

};


#endif //YOUR_PROJECT_NAME_TESTCLUSERINGWMS_H
