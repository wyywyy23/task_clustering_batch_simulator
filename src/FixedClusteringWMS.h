
#ifndef TASK_CLUSTERING_FOR_BATCH_CLUSTERINGWMS_H
#define TASK_CLUSTERING_FOR_BATCH_CLUSTERINGWMS_H

#include <wrench-dev.h>

using namespace wrench;


    class FixedClusteringWMS : public WMS {

    public:

        FixedClusteringWMS(std::string hostname, StandardJobScheduler *job_scheduler, BatchService *batch_service);
        int main() override;

        void processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent>) override;
        void processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent>) override;

    private:
        BatchService *batch_service;
        std::string task_clustering_algorithm;

        PilotJob *pending_pilot_job;
        PilotJob *running_pilot_job;

    };



#endif //YTASK_CLUSTERING_FOR_BATCH_CLUSTERINGWMS_H
