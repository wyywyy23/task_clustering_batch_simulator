/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef YOUR_PROJECT_NAME_ONEJOBCLUSERINGWMS_H
#define YOUR_PROJECT_NAME_ONEJOBCLUSERINGWMS_H


#include <wms/WMS.h>
#include <services/compute/batch/BatchService.h>


namespace wrench {

    class PlaceHolderJob;

    class OneJobClusteringWMS : public WMS {

    public:

        OneJobClusteringWMS(std::string hostname, BatchService *batch_service);

    private:

        BatchService *batch_service;

        int main() override;
        void submitSingleJob();
        double computeWorkflowMakespan(unsigned long num_hosts);
        void processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> e) override;
        double core_speed;
        unsigned long num_hosts;

        std::shared_ptr<JobManager> job_manager;


    };

};


#endif //YOUR_PROJECT_NAME_ZHANGCLUSERINGWMS_H
