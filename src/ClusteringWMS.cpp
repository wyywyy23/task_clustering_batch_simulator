
//
// Created by Henri Casanova on 3/29/18.
//

#include "ClusteringWMS.h"

using namespace wrench;

XBT_LOG_NEW_DEFAULT_CATEGORY(clustering_wms, "Log category for Clustering WMS");

ClusteringWMS::ClusteringWMS(std::string hostname, StandardJobScheduler *standard_job_scheduler, BatchService *batch_service) :
        WMS(std::unique_ptr<StandardJobScheduler>(standard_job_scheduler), nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
  this->batch_service = batch_service;
  this->task_clustering_algorithm = task_clustering_algorithm;
}

int ClusteringWMS::main() {

  this->checkDeferredStart();

  TerminalOutput::setThisProcessLoggingColor(WRENCH_LOGGING_COLOR_RED);
  WRENCH_INFO("Starting!");

  WRENCH_INFO("About to execute a workflow with %lu tasks", this->workflow->getNumberOfTasks());

  // Create a job manager
  std::shared_ptr<JobManager> job_manager = this->createJobManager();

  while (true) {

    // Get the ready tasks
    std::map<std::string, std::vector<wrench::WorkflowTask *>> ready_tasks = this->workflow->getReadyTasks();

    // Scheduler ready tasks
    WRENCH_INFO("Scheduling tasks...");
    this->standard_job_scheduler->scheduleTasks(
            {this->batch_service},
            ready_tasks);

    // Wait for a workflow execution event, and process it
    try {
      this->waitForAndProcessNextEvent();
    } catch (WorkflowExecutionException &e) {
      WRENCH_INFO("Error while getting next execution event (%s)... ignoring and trying again",
                  (e.getCause()->toString().c_str()));
      continue;
    }
    if (workflow->isDone()) {
      break;
    }
  }

  job_manager.reset();
  WRENCH_INFO("WORKFLOW EXECUTION COMPLETE");

  return 0;
}

void ClusteringWMS::processEventStandardJobCompletion(std::unique_ptr<WorkflowExecutionEvent>) {
  WRENCH_INFO("A STANDARD JOB HAS COMPLETED");
}
