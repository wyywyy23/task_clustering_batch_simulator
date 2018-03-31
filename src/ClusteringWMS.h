
//
// Created by Henri Casanova on 3/29/18.
//

#ifndef YOUR_PROJECT_NAME_CLUSTERINGWMS_H
#define YOUR_PROJECT_NAME_CLUSTERINGWMS_H

#include <wrench-dev.h>

using namespace wrench;


    class ClusteringWMS : public WMS {

    public:

        ClusteringWMS(std::string hostname, StandardJobScheduler *job_scheduler, BatchService *batch_service);
        int main() override;

        void processEventStandardJobCompletion(std::unique_ptr<WorkflowExecutionEvent>) override;

    private:
        BatchService *batch_service;
        std::string task_clustering_algorithm;

    };



#endif //YOUR_PROJECT_NAME_CLUSTERINGWMS_H
