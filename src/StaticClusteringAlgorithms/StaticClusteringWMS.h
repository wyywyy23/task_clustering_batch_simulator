
#ifndef TASK_CLUSTERING_FOR_BATCH_CLUSTERINGWMS_H
#define TASK_CLUSTERING_FOR_BATCH_CLUSTERINGWMS_H

#include <wrench-dev.h>
#include "Simulator.h"
#include "ClusteredJob.h"

using namespace wrench;

class StaticClusteringWMS : public WMS {

public:

    StaticClusteringWMS(Simulator *simulator, std::string hostname, BatchService *batch_service,
                        unsigned long max_num_jobs, std::string algorithm_spec);

    int main() override;

    void processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent>) override;

    void processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent>) override;

    static std::set<ClusteredJob *>
    createHCJobs(std::string vc, unsigned long num_tasks_per_cluster, unsigned long num_nodes_per_cluster,
                 Workflow *workflow, unsigned long start_level, unsigned long end_level);

    static std::set<ClusteredJob *>
    createDFJSJobs(std::string vc, unsigned long num_seconds_per_cluster, unsigned long num_nodes_per_cluster,
                   double core_speed, Workflow *workflow, unsigned long start_level, unsigned long end_level);

    static std::set<ClusteredJob *>
    createHRBJobs(std::string vc, unsigned long num_seconds_per_cluster, unsigned long num_nodes_per_cluster,
                  double core_speed, Workflow *workflow, unsigned long start_level, unsigned long end_level);

    static std::set<ClusteredJob *>
    createHIFBJobs(std::string vc, unsigned long num_seconds_per_cluster, unsigned long num_nodes_per_cluster,
                   Workflow *workflow, unsigned long start_level, unsigned long end_level);

    static std::set<ClusteredJob *>
    createHDBJobs(std::string vc, unsigned long num_seconds_per_cluster, unsigned long num_nodes_per_cluster,
                  Workflow *workflow, unsigned long start_level, unsigned long end_level);

private:
    std::set<ClusteredJob *> createClusteredJobs();

    std::set<ClusteredJob *> createVCJobs();

    static std::set<ClusteredJob *> applyPosteriorVC(Workflow *workflow, std::set<ClusteredJob *>);

    static void mergeSingleParentSingleChildPairs(Workflow *workflow);

    static bool areJobsMergable(Workflow *workflow, ClusteredJob *j1, ClusteredJob *j2);

    static bool isSingleParentSingleChildPair(Workflow *workflow, ClusteredJob *pj, ClusteredJob *cj);

    void submitClusteredJob(ClusteredJob *clustered_job);

    std::map<wrench::StandardJob *, ClusteredJob *> job_map;

    Simulator *simulator;

    BatchService *batch_service;
    unsigned long number_of_nodes;

    unsigned long max_num_jobs;
    unsigned long num_jobs_in_systems;
    std::string algorithm_spec;
    double core_speed = 0.0;

    std::shared_ptr<JobManager> job_manager;


};


#endif //YTASK_CLUSTERING_FOR_BATCH_CLUSTERINGWMS_H
