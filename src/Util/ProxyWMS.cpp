//
// Created by evan on 8/26/19.
//

#include <wms/WMS.h>
#include <Simulator.h>
#include <services/compute/batch/BatchComputeService.h>
#include "ProxyWMS.h"
#include "PlaceHolderJob.h"

namespace wrench {

    ProxyWMS::ProxyWMS(Workflow *workflow, std::shared_ptr<JobManager> job_manager,
                       std::shared_ptr<BatchComputeService> batch_service) {
        this->workflow = workflow;
        this->job_manager = job_manager;
        this->batch_service = batch_service;
    }

    PlaceHolderJob *ProxyWMS::createAndSubmitPlaceholderJob(double requested_execution_time,
                                                            unsigned long requested_parallelism,
                                                            unsigned long start_level,
                                                            unsigned long end_level) {

        requested_execution_time = requested_execution_time * EXECUTION_TIME_FUDGE_FACTOR;

        // Aggregate tasks
        std::vector < WorkflowTask * > tasks;
        for (unsigned long l = start_level; l <= end_level; l++) {
            std::vector < WorkflowTask * > tasks_in_level = this->workflow->getTasksInTopLevelRange(l, l);
            for (auto t : tasks_in_level) {
                if (t->getState() != WorkflowTask::COMPLETED) {
                    tasks.push_back(t);
                }
            }
        }

        // Submit the pilot job
        std::map <std::string, std::string> service_specific_args;
        service_specific_args["-N"] = std::to_string(requested_parallelism);
        service_specific_args["-c"] = "1";
        service_specific_args["-t"] = std::to_string(1 + ((unsigned long) requested_execution_time) / 60);

        PlaceHolderJob *pj = new PlaceHolderJob(this->job_manager->createPilotJob(), tasks, start_level, end_level,
                                                requested_execution_time);

        /**
        WRENCH_INFO("Submitting a Pilot Job (%ld hosts, %.2lf sec) for workflow levels %ld-%ld (%s)",
                    requested_parallelism, requested_execution_time, start_level, end_level,
                    pj->pilot_job->getName().c_str());

        WRENCH_INFO("This pilot job has these tasks:");
        for (auto t : pj->tasks) {
            WRENCH_INFO("     - %s", t->getID().c_str());
        }
        */

        this->job_manager->submitJob(pj->pilot_job, this->batch_service, service_specific_args);

        return pj;
    }

}