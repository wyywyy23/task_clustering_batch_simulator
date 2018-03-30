
//
// Created by Henri Casanova on 3/29/18.
//

#include "ClusteringWMS.h"

ClusteringWMS::ClusteringWMS(std::string hostname, ComputeService *batch_service) :
        WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
  this->batch_service = batch_service;
}

int ClusteringWMS::main() {

  std::cerr << "WMS starting!\n";
  Simulation::sleep(10);
  std::cerr << "THE BATCH SERVICE HAS THIS MANY HOSTS: " << this->batch_service->getNumHosts() << "\n";
  return 0;

}