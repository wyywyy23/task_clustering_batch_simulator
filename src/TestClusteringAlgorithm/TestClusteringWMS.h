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
#include <services/compute/batch/BatchService.h>


namespace wrench {

    class Simulator;

    class TestPlaceHolderJob;

    class TestClusteringWMS : public WMS {

    public:

        TestClusteringWMS(Simulator *simulator, std::string hostname, bool overlap, bool plimit, double waste_bound,
                          double beat_bound, BatchService *batch_service);

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


    };

};


#endif //YOUR_PROJECT_NAME_TESTCLUSERINGWMS_H
