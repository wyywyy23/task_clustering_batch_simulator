/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include <wrench-dev.h>
#include <Util/WorkflowUtil.h>
#include <Simulator.h>
#include "EvanClusteringWMS.h"
#include "EvanPlaceHolderJob.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(evan_clustering_wms,
"Log category for Evan Clustering WMS");

#define EXECUTION_TIME_FUDGE_FACTOR 1.1

namespace wrench {

    class Simulator;

    static double parent_runtime = 0;
    static int sequence = 0;

    EvanClusteringWMS::EvanClusteringWMS(Simulator *simulator, std::string hostname, bool overlap, bool plimit,
                                           BatchService *batch_service) :
            WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
        this->simulator = simulator;
        this->overlap = overlap;
        this->plimit = plimit;
        this->batch_service = batch_service;
        this->pending_placeholder_job = nullptr;
        this->individual_mode = false;
    }

    int EvanClusteringWMS::main() {

        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_WHITE);

        this->checkDeferredStart();

        // Find out core speed on the batch service
        this->core_speed = (*(this->batch_service->getCoreFlopRate().begin())).second;
        // Find out #hosts on the batch service
        this->number_of_hosts = this->batch_service->getNumHosts();

        // Create a job manager
        this->job_manager = this->createJobManager();

        while (not this->getWorkflow()->isDone()) {

            // Submit a pilot job (if needed)
            applyGroupingHeuristic();

            this->waitForAndProcessNextEvent();

        }
        return 0;
    }

    void EvanClusteringWMS::applyGroupingHeuristic() {

//      WRENCH_INFO("APPLYING GROUPING HEURISTIC");

        // Don't schedule a pilot job if one is pending
        if (this->pending_placeholder_job) {
            return;
        }

        // Don't schedule a pilot job if we're in individual mode
        if (this->individual_mode) {
            return;
        }

        // Don't schedule a pilot job is overlap = false and anything is running
        if ((not this->overlap) and (not this->running_placeholder_jobs.empty())) {
            return;
        }

        unsigned long start_level = getStartLevel();
        unsigned long end_level = this->getWorkflow()->getNumLevels() - 1;

        // Nothing to do?
        if (start_level > end_level) {
            return;
        }

        // See if we can do better by grouping (Fig. 5 in the paper)
        // return params: wait_time, makespan, end_level
        std::tuple<double, double, unsigned long, unsigned long> partial_dag = groupLevels(start_level, end_level);
        // Don't even need wait time to submit job!
        double partial_dag_wait_time = std::get<0>(partial_dag);
        double partial_dag_makespan = std::get<1>(partial_dag);
        unsigned long partial_dag_end_level = std::get<2>(partial_dag);
        unsigned long partial_dag_parallelism = std::get<3>(partial_dag);

        if (partial_dag_end_level >= end_level) {
            // this->individual_mode = true;
            // come up with a heuristic for picking one_job_per_task?
        }

        WRENCH_INFO("GROUPING: %ld-%ld", start_level, end_level);
        std::cout << "submitting placeholder" << std::endl;
        std::cout << "makespan: " << partial_dag_makespan << std::endl;
        std::cout << "parallelism: " << partial_dag_parallelism << std::endl;
        std::cout << "start: " << start_level << std::endl;
        std::cout << "end: " << partial_dag_end_level << std::endl;

        // createAndSubmitPlaceholderJob(
        //         partial_dag_makespan,
        //         partial_dag_parallelism,
        //         start_level,
        //         partial_dag_end_level);

        // WRENCH_INFO("Switching to individual mode!");
        // // Submit all READY tasks as individual jobs
        // for (auto task : this->getWorkflow()->getTasks()) {
        //     if (task->getState() == WorkflowTask::State::READY) {
        //         StandardJob *standard_job = this->job_manager->createStandardJob(task, {});
        //         std::map <std::string, std::string> service_specific_args;
        //         unsigned long requested_execution_time =
        //                 (task->getFlops() / this->core_speed) * EXECUTION_TIME_FUDGE_FACTOR;
        //         service_specific_args["-N"] = "1";
        //         service_specific_args["-c"] = "1";
        //         service_specific_args["-t"] = std::to_string(1 + ((unsigned long) requested_execution_time) / 60);
        //         WRENCH_INFO("Submitting task %s individually!", task->getID().c_str());
        //         this->job_manager->submitJob(standard_job, this->batch_service, service_specific_args);
        //     }
        // }
    }

    /**
     *
     * @param requested_execution_time
     * @param requested_parallelism
     * @param start_level
     * @param end_level
     */
    void EvanClusteringWMS::createAndSubmitPlaceholderJob(
            double requested_execution_time,
            unsigned long requested_parallelism,
            unsigned long start_level,
            unsigned long end_level) {

        requested_execution_time = requested_execution_time * EXECUTION_TIME_FUDGE_FACTOR;

        // Set global parent runtime to use for leeway calculation
        parent_runtime = requested_execution_time;

        // Aggregate tasks
        std::vector < WorkflowTask * > tasks;
        for (unsigned long l = start_level; l <= end_level; l++) {
            std::vector < WorkflowTask * > tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(l, l);
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


        // Keep track of the placeholder job
        this->pending_placeholder_job = new EvanPlaceHolderJob(
                this->job_manager->createPilotJob(),
                tasks,
                start_level,
                end_level);

        WRENCH_INFO("Submitting a Pilot Job (%ld hosts, %.2lf sec) for workflow levels %ld-%ld (%s)",
                    requested_parallelism, requested_execution_time, start_level, end_level,
                    this->pending_placeholder_job->pilot_job->getName().c_str());
        WRENCH_INFO("This pilot job has these tasks:");
        for (auto t : this->pending_placeholder_job->tasks) {
            WRENCH_INFO("     - %s", t->getID().c_str());
        }

        // submit the corresponding pilot job
        this->job_manager->submitJob(this->pending_placeholder_job->pilot_job, this->batch_service,
                                     service_specific_args);
    }


    void EvanClusteringWMS::processEventPilotJobStart(std::unique_ptr <PilotJobStartedEvent> e) {

        // Update queue waiting time
        this->simulator->total_queue_wait_time +=
                this->simulation->getCurrentSimulatedDate() - e->pilot_job->getSubmitDate();

        // Just for kicks, check it was the pending one
        WRENCH_INFO("Got a Pilot Job Start event: %s", e->pilot_job->getName().c_str());
        if (this->pending_placeholder_job == nullptr) {
            throw std::runtime_error("Fatal Error: couldn't find a placeholder job for a pilob job that just started");
        }
//      WRENCH_INFO("Got a Pilot Job Start event e->pilot_job = %ld, this->pending->pilot_job = %ld (%s)",
//                  (unsigned long) e->pilot_job,
//                  (unsigned long) this->pending_placeholder_job->pilot_job,
//                  this->pending_placeholder_job->pilot_job->getName().c_str());

        if (e->pilot_job != this->pending_placeholder_job->pilot_job) {

            WRENCH_INFO("Must be for a placeholder I already cancelled... nevermind");
            return;
        }

        EvanPlaceHolderJob *placeholder_job = this->pending_placeholder_job;

        // Move it to running
        this->running_placeholder_jobs.insert(placeholder_job);
        this->pending_placeholder_job = nullptr;

        // Submit all ready tasks to it each in its standard job
        std::string output_string = "";
        for (auto task : placeholder_job->tasks) {
            if (task->getState() == WorkflowTask::READY) {
                StandardJob *standard_job = this->job_manager->createStandardJob(task, {});
                output_string += " " + task->getID();

                WRENCH_INFO("Submitting task %s as part of placeholder job %ld-%ld",
                            task->getID().c_str(), placeholder_job->start_level, placeholder_job->end_level);
                this->job_manager->submitJob(standard_job, placeholder_job->pilot_job->getComputeService());
            }
        }

        // Re-submit a pilot job so as to overlap execution of job n with waiting of job n+1
        this->applyGroupingHeuristic();

    }

    void EvanClusteringWMS::processEventPilotJobExpiration(std::unique_ptr <PilotJobExpiredEvent> e) {
        std::cout << "JOB EXPIRATION!!!" << std::endl;

        // Find the placeholder job
        EvanPlaceHolderJob *placeholder_job = nullptr;
        for (auto ph : this->running_placeholder_jobs) {
            if (ph->pilot_job == e->pilot_job) {
                placeholder_job = ph;
                break;
            }
        }
        if (placeholder_job == nullptr) {
            throw std::runtime_error("Got a pilot job expiration, but no matching placeholder job found");
        }

        // Remove it from the list of running pilot jobs
        this->running_placeholder_jobs.erase(placeholder_job);

        WRENCH_INFO("Got a pilot job expiration for a placeholder job that deals with levels %ld-%ld (%s)",
                    placeholder_job->start_level, placeholder_job->end_level,
                    placeholder_job->pilot_job->getName().c_str());
        // Check if there are unprocessed tasks
        bool unprocessed = false;
        for (auto t : placeholder_job->tasks) {
            if (t->getState() != WorkflowTask::COMPLETED) {
                unprocessed = true;
                break;
            }
        }

//      double wasted_node_seconds = e->pilot_job->getNumHosts() * e->pilot_job->getDuration();
        unsigned long num_used_nodes;
        sscanf(e->pilot_job->getServiceSpecificArguments()["-N"].c_str(), "%lu", &num_used_nodes);
        unsigned long num_used_minutes;
        sscanf(e->pilot_job->getServiceSpecificArguments()["-t"].c_str(), "%lu", &num_used_minutes);
        double wasted_node_seconds = 60.0 * num_used_minutes * num_used_nodes;
        for (auto t : placeholder_job->tasks) {
            if (t->getState() == WorkflowTask::COMPLETED) {
                wasted_node_seconds -= t->getFlops() / this->core_speed;
            }
        }
        this->simulator->wasted_node_seconds += wasted_node_seconds;

        if (not unprocessed) { // Nothing to do
            WRENCH_INFO("This placeholder job has no unprocessed tasks. great.");
            return;
        }

        this->simulator->num_pilot_job_expirations_with_remaining_tasks_to_do++;

        WRENCH_INFO("This placeholder job has unprocessed tasks");

        // Cancel pending pilot job if any
        if (this->pending_placeholder_job) {
            WRENCH_INFO("Canceling pending placeholder job (placeholder=%ld,  pilot_job=%ld / %s",
                        (unsigned long) this->pending_placeholder_job,
                        (unsigned long) this->pending_placeholder_job->pilot_job,
                        this->pending_placeholder_job->pilot_job->getName().c_str());
            this->job_manager->terminateJob(this->pending_placeholder_job->pilot_job);
            this->pending_placeholder_job = nullptr;
        }

        // Cancel running pilot jobs if none of their tasks has started

        std::set < EvanPlaceHolderJob * > to_remove;
        for (auto ph : this->running_placeholder_jobs) {
            bool started = false;
            for (auto task : ph->tasks) {
                if (task->getState() != WorkflowTask::NOT_READY) {
                    started = true;
                }
            }
            if (not started) {
                WRENCH_INFO("Canceling running placeholder job that handled levels %ld-%ld because none"
                            "of its tasks has started (%s)", ph->start_level, ph->end_level,
                            ph->pilot_job->getName().c_str());
                try {
                    this->job_manager->terminateJob(ph->pilot_job);
                } catch (WorkflowExecutionException &e) {
                    // ignore (likely already dead!)
                }
                to_remove.insert(ph);
            }
        }

        for (auto ph : to_remove) {
            this->running_placeholder_jobs.erase(ph);
        }

        // Make decisions again
        applyGroupingHeuristic();

    }

    void EvanClusteringWMS::processEventStandardJobCompletion(std::unique_ptr <StandardJobCompletedEvent> e) {

        WorkflowTask *completed_task = e->standard_job->tasks[0]; // only one task per job

        WRENCH_INFO("Got a standard job completion for task %s", completed_task->getID().c_str());

        this->simulator->used_node_seconds += completed_task->getFlops() / this->core_speed;

        // Find the placeholder job this task belongs to
        EvanPlaceHolderJob *placeholder_job = nullptr;
        for (auto ph : this->running_placeholder_jobs) {
            for (auto task : ph->tasks) {
                if (task == completed_task) {
                    placeholder_job = ph;
                    break;
                }
            }
        }

        if ((placeholder_job == nullptr) and (not this->individual_mode)) {
            throw std::runtime_error("Got a task completion, but couldn't find a placeholder for the task, "
                                     "and we're not in individual mode");
        }

        if (placeholder_job != nullptr) {

            // Terminate the pilot job in case all its tasks are done
            bool all_tasks_done = true;
            for (auto t : placeholder_job->tasks) {
                if (t->getState() != WorkflowTask::COMPLETED) {
                    all_tasks_done = false;
                    break;
                }
            }
            if (all_tasks_done) {
                WRENCH_INFO("All tasks are completed in this placeholder job, so I am terminating it (%s)",
                            placeholder_job->pilot_job->getName().c_str());
                try {
                    WRENCH_INFO("TERMINATING A PILOT JOB");
                    this->job_manager->terminateJob(placeholder_job->pilot_job);
                } catch (WorkflowExecutionException &e) {
                    // ignore
                }
                this->running_placeholder_jobs.erase(placeholder_job);
            }


        }

        // Start all newly ready tasks that depended on the completed task, IN ANY PLACEHOLDER
        // This shouldn't happen in individual mode, but can't hurt
        std::vector < WorkflowTask * > children = this->getWorkflow()->getTaskChildren(completed_task);
        for (auto ph : this->running_placeholder_jobs) {
            for (auto task : ph->tasks) {
                if ((std::find(children.begin(), children.end(), task) != children.end()) and
                    (task->getState() == WorkflowTask::READY)) {
                    StandardJob *standard_job = this->job_manager->createStandardJob(task, {});
                    WRENCH_INFO("Submitting task %s  as part of placeholder job %ld-%ld",
                                task->getID().c_str(), placeholder_job->start_level, placeholder_job->end_level);
                    this->job_manager->submitJob(standard_job, ph->pilot_job->getComputeService());
                }
            }
        }

        if (this->individual_mode) {
            for (auto task : this->getWorkflow()->getTasks()) {
                if (task->getState() == WorkflowTask::State::READY) {
                    StandardJob *standard_job = this->job_manager->createStandardJob(task, {});
                    WRENCH_INFO("Submitting task %s individually!",
                                task->getID().c_str());
                    std::map <std::string, std::string> service_specific_args;
                    double requested_execution_time =
                            (task->getFlops() / this->core_speed) * EXECUTION_TIME_FUDGE_FACTOR;
                    service_specific_args["-N"] = "1";
                    service_specific_args["-c"] = "1";
                    service_specific_args["-t"] = std::to_string(1 + ((unsigned long) requested_execution_time) / 60);
                    this->job_manager->submitJob(standard_job, this->batch_service, service_specific_args);
                }
            }
        }


    }

    void EvanClusteringWMS::processEventStandardJobFailure(std::unique_ptr <StandardJobFailedEvent> e) {
        WRENCH_INFO("Got a standard job failure event for task %s -- IGNORING THIS",
                    e->standard_job->tasks[0]->getID().c_str());
    }

    double EvanClusteringWMS::estimateWaitTime(long parallelism, double makespan, int *sequence) {
        std::set <std::tuple<std::string, unsigned int, unsigned int, double>> job_config;
        std::string config_key = "config_XXXX_" + std::to_string((*sequence)++); // need to make it unique for BATSCHED
        job_config.insert(std::make_tuple(config_key, (unsigned int) parallelism, 1, makespan));
        std::map<std::string, double> estimates = this->batch_service->getStartTimeEstimates(job_config);

        if (estimates[config_key] < 0) {
            throw std::runtime_error("Could not obtain start time estimate... aborting");
        }

        double wait_time_estimate = std::max<double>(0, estimates[config_key] -
                                                        this->simulation->getCurrentSimulatedDate());
        return wait_time_estimate;
    }

    // Compute my start level first as the first level that's not fully completed
    unsigned long EvanClusteringWMS::getStartLevel() {
        unsigned long start_level = 0;
        for (unsigned long i = 0; i < this->getWorkflow()->getNumLevels(); i++) {
            std::vector < WorkflowTask * > tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(i, i);
            bool all_completed = true;
            for (auto task : tasks_in_level) {
                if (task->getState() != WorkflowTask::State::COMPLETED) {
                    all_completed = false;
                }
            }
            if (all_completed) {
                start_level = i + 1;
            }
        }

        for (auto ph : this->running_placeholder_jobs) {
            start_level = 1 + std::max<unsigned long>(start_level, ph->end_level);
        }

        return start_level;
    }

    // Zhang is supposed to fail automatically if number of tasks > number of hosts
    // Just return max hosts to avoid failure for now
    // end_level = num_levels - 1
    // For evan algorithm, find the best parallelism to minimize wait and makespan
    unsigned long EvanClusteringWMS::maxParallelism(unsigned long start_level, unsigned long end_level) {
        unsigned long max_parallelism = 0;
        for (unsigned long i = start_level; i <= end_level; i++) {
            unsigned long num_tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(i, i).size();
            max_parallelism = std::max<unsigned long>(max_parallelism, num_tasks_in_level);
        }
        max_parallelism = std::min<unsigned long>(max_parallelism, this->number_of_hosts);

        unsigned long best_parallelism = 0;
        double best_total_time = DBL_MAX;
        for (unsigned long i = 1; i < max_parallelism + 1; i++) {
            double makespan = WorkflowUtil::estimateMakespan(
                    this->getWorkflow()->getTasksInTopLevelRange(start_level, end_level),
                    i, this->core_speed);
            double wait_time = estimateWaitTime(i, makespan, &sequence);
            double total_time = makespan + wait_time;
            if (total_time < best_total_time) {
                best_total_time = total_time;
                best_parallelism = i;
            }
        }

        return best_parallelism;
    }

    // return params: wait_time, run_time, end_level, parallelism
    std::tuple<double, double, unsigned long, unsigned long>
    EvanClusteringWMS::groupLevels(unsigned long start_level, unsigned long end_level) {

        // automatically add leeway to first level grouping
        unsigned long candidate_end_level = start_level;
        unsigned long best_parallelism = -1;
        double best_runtime = -1;
        double best_wait_time = DBL_MAX;

        for (unsigned long i = start_level; i < end_level - 1; i++) {
            std::cout << "start: " << start_level << " end: " << i << std::endl;
            unsigned long parallelism = maxParallelism(start_level, i);
            double runtime = WorkflowUtil::estimateMakespan(
                this->getWorkflow()->getTasksInTopLevelRange(start_level, i),
                    parallelism, this->core_speed);
            double wait_time = estimateWaitTime(parallelism, runtime, &sequence);

            // consider the leeway
            if (wait_time < parent_runtime) {
                double leeway = parent_runtime - wait_time;
                if (leeway > (runtime * 0.10)) {
                    // leeway wastes resources
                    continue;
                } else {
                    runtime += leeway;
                    wait_time = estimateWaitTime(parallelism, runtime, &sequence);
                }
            }
            // borrowing this concept from zhang
            double real_wait_time = wait_time;
            if (parent_runtime <= wait_time) {
                real_wait_time = wait_time - parent_runtime;
            }
            double current_ratio = runtime / real_wait_time;
            double best_ratio = best_runtime / wait_time;
            if (current_ratio >= best_ratio) {
                best_runtime = runtime;
                best_wait_time = real_wait_time;
                best_parallelism = parallelism;
                candidate_end_level = i;
            }
        }

        // calculate wait time if using "real wait time"
        best_wait_time = estimateWaitTime(best_parallelism, best_runtime, &sequence);

        std::cout << "wait: " << best_wait_time << std::endl;
        std::cout << "run: " << best_runtime << std::endl;
        std::cout << "parallelism: " << best_parallelism << std::endl;

        return std::make_tuple(best_wait_time, best_runtime, candidate_end_level, best_parallelism);
    }

};
