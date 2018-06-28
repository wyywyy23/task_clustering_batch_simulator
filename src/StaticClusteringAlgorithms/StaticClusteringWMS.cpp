

#include <WorkflowUtil/WorkflowUtil.h>
#include "StaticClusteringWMS.h"
#include "ClusteredJob.h"

using namespace wrench;

#define EXECUTION_TIME_FUDGE_FACTOR 60


XBT_LOG_NEW_DEFAULT_CATEGORY(static_clustering_wms, "Log category for Static Clustering WMS");

StaticClusteringWMS::StaticClusteringWMS(std::string hostname, BatchService *batch_service,
                                         unsigned long max_num_jobs, std::string algorithm_spec) :
        WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "static_clustering_wms") {
  this->batch_service = batch_service;
  this->max_num_jobs = max_num_jobs;
  this->algorithm_spec = algorithm_spec;
}


void StaticClusteringWMS::processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> e) {
  StandardJob *job = e->standard_job;
  WRENCH_INFO("Job %s has completed", job->getName().c_str());
  this->num_jobs_in_systems--;
}


void StaticClusteringWMS::processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent> e) {
  throw std::runtime_error("A job has failed, which shouldn't happen");
}


std::set<ClusteredJob *> StaticClusteringWMS::createClusteredJobs() {

  std::istringstream ss(this->algorithm_spec);
  std::string token;
  std::vector<std::string> tokens;

  std::set<ClusteredJob *> jobs;

  while(std::getline(ss, token, '-')) {
    tokens.push_back(token);
  }

  /** A single Job **/
  if (tokens[0] == "one_job") {
    if (tokens.size() != 2) {
      throw std::invalid_argument("Invalid static:one_job specification");
    }

    unsigned long num_nodes;
    if ((sscanf(tokens[1].c_str(), "%lu", &num_nodes) != 1) or (num_nodes < 1)) {
      throw std::invalid_argument("Invalid static:one_job-m specification");
    }
    ClusteredJob *job = new ClusteredJob();
    for (auto t : this->getWorkflow()->getTasks()) {
      job->addTask(t);
    }
    job->setNumNodes(num_nodes);
    jobs.insert(job);
    return jobs;
  }

  /** One Job per Task **/
  if (tokens[0] == "one_job_per_task") {
    if (tokens.size() != 1) {
      throw std::invalid_argument("Invalid static:one_job_per_task specification");
    }

    for (auto t : this->getWorkflow()->getTasks()) {
      ClusteredJob *job = new ClusteredJob();
      job->addTask(t);
      job->setNumNodes(1);
      jobs.insert(job);
    }
    return jobs;
  }

  /** Horizontal Clustering (HC) **/
  if (tokens[0] == "hc") {
    if (tokens.size() != 3) {
      throw std::invalid_argument("Invalid static:hc specification");
    }
    unsigned long num_tasks_per_cluster;
    unsigned long num_nodes_per_cluster;
    if ((sscanf(tokens[1].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
        (sscanf(tokens[2].c_str(), "%lu", &num_nodes_per_cluster) != 1) or (num_nodes_per_cluster < 1)) {
      throw std::invalid_argument("Invalid static:hc specification");
    }

    return createHCJobs(num_tasks_per_cluster, num_nodes_per_cluster);
  }

  /** DFJS Clustering **/
  if (tokens[0] == "dfjs") {
    if (tokens.size() != 3) {
      throw std::invalid_argument("Invalid static:dfjs specification");
    }
    unsigned long num_seconds_per_cluster;
    unsigned long num_nodes_per_cluster;
    if ((sscanf(tokens[1].c_str(), "%lu", &num_seconds_per_cluster) != 1) or (num_seconds_per_cluster < 1) or
        (sscanf(tokens[2].c_str(), "%lu", &num_nodes_per_cluster) != 1) or (num_nodes_per_cluster < 1)) {
      throw std::invalid_argument("Invalid static:hc specification");
    }

    return createDFJSJobs(num_seconds_per_cluster, num_nodes_per_cluster);
  }

  throw std::runtime_error("Unknown Static Job Clustering method " + tokens[0]);


}

int StaticClusteringWMS::main() {

  // Acquire core speed the first time
  if (this->core_speed <= 0.0) {
    this->core_speed = batch_service->getCoreFlopRate()[0];
  }

  this->checkDeferredStart();

  TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_YELLOW);
  WRENCH_INFO("About to execute a workflow with %lu tasks", this->getWorkflow()->getNumberOfTasks());

  // Create a job manager
  this->job_manager = this->createJobManager();

  // Compute the clustering according to the method
  std::set<ClusteredJob *> jobs = this->createClusteredJobs();

  this->num_jobs_in_systems = 0;

  while (true) {

    while (this->num_jobs_in_systems < this->max_num_jobs) {
      // Try to find a ready job
      ClusteredJob *to_submit = nullptr;
      for (auto j : jobs) {
        if (j->isReady()) {
          to_submit = j;
          break;
        }
      }
      if (to_submit == nullptr) {
        break;
      }

      // Submit the job
      submitClusteredJob(to_submit);
      this->num_jobs_in_systems++;
    }

    // Wait for a workflow execution event, and process it
    try {
//      WRENCH_INFO("Waiting for an event");
      this->waitForAndProcessNextEvent();
    } catch (WorkflowExecutionException &e) {
      WRENCH_INFO("Error while getting next execution event (%s)... ignoring and trying again",
                  (e.getCause()->toString().c_str()));
      continue;
    }

    // Are we done?
    if (this->getWorkflow()->isDone()) {
      break;
    }
  }

//  std::cout << "WORKFLOW EXECUTION COMPLETE: " <<  this->simulation->getCurrentSimulatedDate() << "\n";
  job_manager.reset();

  return 0;
}

void StaticClusteringWMS::submitClusteredJob(ClusteredJob *clustered_job) {

  // Compute the number of nodes for the job
  unsigned long num_nodes = std::min<unsigned long>(clustered_job->getNumTasks(), clustered_job->getNumNodes());


  // Compute the time for the job (a bit conservative for now)
  double makespan = WorkflowUtil::estimateMakespan(clustered_job->getTasks(), num_nodes, this->core_speed);

  std::map<std::string, std::string> batch_job_args;
  batch_job_args["-N"] = std::to_string(num_nodes);
  batch_job_args["-t"] = std::to_string((unsigned long)(1 + (makespan + EXECUTION_TIME_FUDGE_FACTOR) / 60.0)); //time in minutes
  batch_job_args["-c"] = "1"; //number of cores per node

  StandardJob *standard_job = this->job_manager->createStandardJob(clustered_job->getTasks(), {});
  WRENCH_INFO("Created a batch job with with batch arguments: %s:%s:%s",
              batch_job_args["-N"].c_str(),
              batch_job_args["-t"].c_str(),
              batch_job_args["-c"].c_str());

  try {
    WRENCH_INFO("Submitting a batch job...");
    this->job_manager->submitJob(standard_job, batch_service, batch_job_args);
//    this->job_map.insert(std::make_pair(standard_job, clustered_job));
  } catch (WorkflowExecutionException &e) {
    throw std::runtime_error("Couldn't submit job: " + e.getCause()->toString());
  }

}

std::set<ClusteredJob *>  StaticClusteringWMS::createHCJobs(unsigned long num_tasks_per_cluster, unsigned long num_nodes_per_cluster) {

  std::set<ClusteredJob *> jobs;

  // Go through each level and creates jobs
  for (unsigned long l = 0; l <= this->getWorkflow()->getNumLevels(); l++) {
    auto tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(l, l);
    ClusteredJob *job = nullptr;
    for (auto t : tasks_in_level) {
      if (job == nullptr) {
        job = new ClusteredJob();
        job->setNumNodes(num_nodes_per_cluster);
      }
      job->addTask(t);
      if (job->getNumTasks() == num_tasks_per_cluster) {
        jobs.insert(job);
        job = nullptr;
      }
    }
    if (job != nullptr) {
      jobs.insert(job);
    }
  }

  return jobs;

}


std::set<ClusteredJob *> StaticClusteringWMS::createDFJSJobs(unsigned long num_seconds_per_cluster, unsigned long num_nodes_per_cluster) {
  std::set<ClusteredJob *> jobs;


  // Go through each level and creates jobs
  for (unsigned long l = 0; l < this->getWorkflow()->getNumLevels(); l++) {
    auto tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(l, l);

//    WRENCH_INFO("DEALING WITH LEVEL %ld", l);
//    WRENCH_INFO("CREATING AN INITIAL JOB");
    auto job = new ClusteredJob();
    job->setNumNodes(num_nodes_per_cluster);
    for (auto t : tasks_in_level) {
      unsigned long task_execution_time = (unsigned long)(ceil(t->getFlops() / this->core_speed));
      if (task_execution_time > num_seconds_per_cluster) {
        throw std::runtime_error("Task " + t->getID() + " by itself takes longer (" + std::to_string(task_execution_time) +
                                         " sec) than the cluster duration upper bound ( " +
                                         std::to_string(num_seconds_per_cluster) + " sec)!");
      }
//      WRENCH_INFO("CONSIDERING TASK %s which takes time %lf", t->getID().c_str(), t->getFlops());
      // Should we add to the job?
      std::vector<wrench::WorkflowTask *> tentative_tasks = job->getTasks();
      tentative_tasks.push_back(t);
//      WRENCH_INFO("ESTIMATING MAKESPAN HERE with %ld nodes", num_nodes_per_cluster);
      double estimated_makespan = WorkflowUtil::estimateMakespan(tentative_tasks, num_nodes_per_cluster, this->core_speed);
      if ((unsigned long)(ceil(estimated_makespan)) <= num_seconds_per_cluster) {
        job->addTask(t);
      } else {
        jobs.insert(job);
        job = new ClusteredJob();
        job->setNumNodes(num_nodes_per_cluster);
        job->addTask(t);
      }
    }
    jobs.insert(job);
  }

  // Sanity check
  for (auto job : jobs) {
    if (job->getNumTasks() == 0) {
      throw std::runtime_error("DFJS Failure: some jobs have no tasks (likely the time bound is too low");
    }
  }

  WRENCH_INFO("DFJS clustering: %ld clusters", jobs.size());
  return jobs;
}





























