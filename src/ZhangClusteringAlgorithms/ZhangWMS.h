/**
 * Copyright (c) 2019. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef TASK_CLUSTERING_BATCH_SIMULATOR_ZHANGWMS_H
#define TASK_CLUSTERING_BATCH_SIMULATOR_ZHANGWMS_H

#include <wrench-dev.h>
#include "Simulator.h"
#include <Util/PlaceHolderJob.h>
#include <Util/ProxyWMS.h>

namespace wrench {

    class ZhangWMS : public WMS {

    public:

        ZhangWMS(Simulator *simulator,
                 std::string hostname,
                 std::shared_ptr<BatchComputeService> batch_service,
                 unsigned long max_num_jobs,
                 bool pick_globally_best_split,
                 bool binary_search_for_leeway,
                 bool calculate_parallelism_based_on_predictions);

    private:

        Simulator *simulator;
        std::shared_ptr<BatchComputeService> batch_service;

        // Allows for variations of zhang
        bool pick_globally_best_split;
        bool binary_search_for_leeway;
        bool calculate_parallelism_based_on_predictions;

        int main() override;

        void applyGroupingHeuristic();

        std::tuple<double, double, double, unsigned long, unsigned long>
        groupLevels(unsigned long start_level, unsigned long end_level);

        unsigned long bestParallelism(unsigned long start_level, unsigned long end_level, bool use_predictions);

        double calculateLeeway(double wait_time, double runtime, unsigned long num_nodes);

        double calculateLeewayBinarySearch(double runtime, unsigned long num_nodes, double parent_runtime, double lower,
                                           double upper);

        double calculateLeewayZhangHeuristic(double wait_time, double runtime, unsigned long num_nodes,
                                             double parent_runtime);

        void processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent> e) override;

        void processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent> e) override;

        void processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> e) override;

        void processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> e) override;

        // std::tuple<double, double, unsigned long, unsigned long> groupLevels(unsigned long start_level, unsigned long end_level);

        bool individual_mode;

        PlaceHolderJob *pending_placeholder_job;
        std::set<PlaceHolderJob *> running_placeholder_jobs;
        double core_speed;
        unsigned long number_of_hosts;
        unsigned long num_jobs_in_system;
        unsigned long max_num_jobs;
        std::shared_ptr<JobManager> job_manager;

        ProxyWMS *proxyWMS;

        // Number of times the workflow was split
        unsigned long number_of_splits;

    };

}

#endif //TASK_CLUSTERING_BATCH_SIMULATOR_ZHANGWMS_H
