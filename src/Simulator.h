

#ifndef TASK_CLUSTERING_BATCH_SIMULATOR_SIMULATOR_H
#define TASK_CLUSTERING_BATCH_SIMULATOR_SIMULATOR_H

#include "wrench-dev.h"

namespace wrench {

    class Simulator {

    public:
        unsigned long num_pilot_job_expirations_with_remaining_tasks_to_do = 0;


        int main(int argc, char **argv);

        void setupSimulationPlatform(wrench::Simulation *simulation, unsigned long num_compute_nodes);

        wrench::Workflow *createWorkflow(std::string workflow_spec);

        wrench::Workflow *createIndepWorkflow(std::vector<std::string> spec_tokens);

        wrench::Workflow *createLevelsWorkflow(std::vector<std::string> spec_tokens);

        wrench::Workflow *createDAXWorkflow(std::vector<std::string> spec_tokens);

        wrench::WMS *
        createWMS(std::string scheduler_spec, wrench::BatchService *batch_service, unsigned long max_num_jobs,
                  std::string algorithm_name);


    };

}

#endif //TASK_CLUSTERING_BATCH_SIMULATOR_SIMULATOR_H
