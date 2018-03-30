
//
// Created by Henri Casanova on 3/29/18.
//

#include "ClusteringWMS.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(clustering_wms, "Log category for Clustering WMS");

ClusteringWMS::ClusteringWMS(std::string hostname, BatchService *batch_service) :
        WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
  this->batch_service = batch_service;
}

int ClusteringWMS::main() {

  this->checkDeferredStart();

  TerminalOutput::setThisProcessLoggingColor(WRENCH_LOGGING_COLOR_RED);
  WRENCH_INFO("Starting!");

  unsigned long num_compute_node = this->batch_service->getNumHosts();
  WRENCH_INFO("The Batch Service has %ld compute nodes", num_compute_node);

  std::tuple<std::string,unsigned int,double> my_job1 = std::make_tuple("config1", 1, 1800);
  std::tuple<std::string,unsigned int,double> my_job2 = std::make_tuple("config2", 2, 900);
  std::set<std::tuple<std::string,unsigned int,double>> set_of_jobs = {my_job1, my_job2};

  std::map<std::string, double> estimates;
  try {
    WRENCH_INFO("Getting queue waiting time estimates");
    estimates = this->batch_service->getQueueWaitingTimeEstimate(set_of_jobs);
  } catch (std::runtime_error &e) {
    throw;
  }

  WRENCH_INFO("ESTIMATE #1: (1,1800): %lf", estimates["config1"]);
  WRENCH_INFO("ESTIMATE #2: (2,900): %lf", estimates["config2"]);

  Simulation::sleep(10);

  return 0;

}