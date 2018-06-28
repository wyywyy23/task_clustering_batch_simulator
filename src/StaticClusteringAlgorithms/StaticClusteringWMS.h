
#ifndef TASK_CLUSTERING_FOR_BATCH_CLUSTERINGWMS_H
#define TASK_CLUSTERING_FOR_BATCH_CLUSTERINGWMS_H

#include <wrench-dev.h>
#include "ClusteredJob.h"

using namespace wrench;


class StaticClusteringWMS : public WMS {

public:

    StaticClusteringWMS(std::string hostname, BatchService *batch_service, unsigned long max_num_jobs, std::string algorithm_spec);
    int main() override;

    void processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent>) override;
    void processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent>) override;

private:

    std::set<ClusteredJob *> createClusteredJobs();
    std::set<ClusteredJob *> createHCJobs(unsigned long num_tasks_per_cluster, unsigned long num_nodes_per_cluster);
    std::set<ClusteredJob *> createDFJSJobs(unsigned long num_seconds_per_cluster, unsigned long num_nodes_per_cluster);
    std::set<ClusteredJob *> createHRBJobs(unsigned long num_seconds_per_cluster, unsigned long num_nodes_per_cluster);
    std::set<ClusteredJob *> createHIFBJobs(unsigned long num_seconds_per_cluster, unsigned long num_nodes_per_cluster);
    void submitClusteredJob(ClusteredJob *clustered_job);
    std::map<wrench::StandardJob *, ClusteredJob *> job_map;

    BatchService *batch_service;
    unsigned long max_num_jobs;
    unsigned long num_jobs_in_systems;
    std::string algorithm_spec;
    double core_speed = 0.0;

    std::shared_ptr<JobManager> job_manager;


};



#endif //YTASK_CLUSTERING_FOR_BATCH_CLUSTERINGWMS_H
