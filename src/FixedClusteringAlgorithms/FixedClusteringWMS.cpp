

#include "FixedClusteringWMS.h"
#include "FixedClusteringScheduler.h"

using namespace wrench;

XBT_LOG_NEW_DEFAULT_CATEGORY(fixed_clustering_wms, "Log category for Fixed Clustering WMS");

FixedClusteringWMS::FixedClusteringWMS(std::string hostname, StandardJobScheduler *standard_job_scheduler, BatchService *batch_service) :
        WMS(std::unique_ptr<StandardJobScheduler>(standard_job_scheduler), nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
  this->batch_service = batch_service;
  this->task_clustering_algorithm = task_clustering_algorithm;
}

int FixedClusteringWMS::main() {

  this->checkDeferredStart();

  TerminalOutput::setThisProcessLoggingColor(COLOR_YELLOW);
  WRENCH_INFO("Starting!");

  WRENCH_INFO("About to execute a workflow with %lu tasks", this->workflow->getNumberOfTasks());

  // Create a job manager
  std::shared_ptr<JobManager> job_manager = this->createJobManager();

  while (true) {

//    this->pilot_job_scheduler->schedulePilotJobs({this->batch_service});

    // Get the ready tasks
    std::map<std::string, std::vector<wrench::WorkflowTask *>> ready_tasks = this->workflow->getReadyTasks();

    // Schedule ready tasks
    WRENCH_INFO("Scheduling tasks %ld ready tasks...", ready_tasks.size());
    this->standard_job_scheduler->scheduleTasks(
            {this->batch_service},
            ready_tasks);

    // Wait for a workflow execution event, and process it
    try {
      WRENCH_INFO("Waiting for an event");
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

  std::cout << "WORKFLOW EXECUTION COMPLETE: " <<  this->simulation->getCurrentSimulatedDate() << "\n";
  job_manager.reset();

  return 0;
}

void FixedClusteringWMS::processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> e) {
  StandardJob *job = e->standard_job;
  WRENCH_INFO("Job %s has completed", job->getName().c_str());
  // Remove the job from the set of pending jobs
  ((FixedClusteringScheduler *)(this->standard_job_scheduler.get()))->submitted_jobs.erase(job);
}


void FixedClusteringWMS::processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent> e) {
  WRENCH_INFO("A job has failed");
}





























