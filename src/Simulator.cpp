//
// Created by Henri Casanova on 3/20/18.
//

#include <iostream>
#include <wrench-dev.h>
#include <services/compute/batch/BatchServiceProperty.h>
#include "ClusteringWMS.h"
#include "FixedClusteringScheduler.h"

using namespace wrench;

void setupSimulationPlatform(Simulation *simulation, int num_compute_nodes);
Workflow *createWorkflow(std::string workflow_spec);
Workflow *createIndepWorkflow(std::vector<std::string> spec_tokens);
StandardJobScheduler *createStandardJobScheduler(std::string scheduler_spec);

int main(int argc, char **argv) {

  // Create and initialize a simulation
  auto simulation = new wrench::Simulation();
  simulation->init(&argc, argv);

  // Parse command-line arguments
  if (argc != 6) {
    std::cerr << "Usage: " << argv[0] << " <num_compute_nodes> <SWF job trace file> <workflow specification> <workflow start time> <algorithm>" << "\n";
    std::cerr << "  * workflow specification options:" << "\n";
    std::cerr << "    - indep:n:t1:t2" << "\n";
    std::cerr << "      - n: number of tasks" << "\n";
    std::cerr << "      - t1/t2: min/max task durations in integral second (uniformly distributed)" << "\n";
    std::cerr << "  * algorithm  options:" << "\n";
    std::cerr << "    - fixed_clustering:n" << "\n";
    std::cerr << "      - n: number of tasks in each cluster" << "\n";
    exit(1);
  }
  int num_compute_nodes;
  if ((sscanf(argv[1], "%d", &num_compute_nodes) != 1) or (num_compute_nodes < 1)) {
    std::cerr << "Invalid number of compute nodes\n";
  }

  double workflow_start_time;
  if ((sscanf(argv[4], "%lf", &workflow_start_time) != 1) or (workflow_start_time < 0)) {
    std::cerr << "Invalid workflow start time\n";
  }


  // Setup the simulation platform
  setupSimulationPlatform(simulation, num_compute_nodes);

  // Create a BatchService
  std::vector<std::string> compute_nodes;
  for (int i=0; i < num_compute_nodes; i++) {
    compute_nodes.push_back("ComputeNode_" + std::to_string(i));
  }
  BatchService *batch_service;
  try {
    std::string login_hostname = "Login";
    batch_service = new BatchService(login_hostname, true, false, compute_nodes, nullptr,
//                                     {{BatchServiceProperty::BATCH_SCHEDULING_ALGORITHM, "FCFS"},
                                     {{BatchServiceProperty::BATCH_SCHEDULING_ALGORITHM, "conservative_bf"},
                                      {BatchServiceProperty::SIMULATED_WORKLOAD_TRACE_FILE, argv[2]}
                                     });
    simulation->add(batch_service);
  } catch (std::invalid_argument &e) {
    std::cerr << "Cannot instantiate batch service: " << e.what() << "\n";
    exit(1);
  }

  // Create the Standard Job Scheduler
  StandardJobScheduler *scheduler = createStandardJobScheduler(argv[5]);


  // Create the WMS
  WMS *wms = nullptr;
  try {
    wms = simulation->add(new ClusteringWMS("Login", scheduler, batch_service));
  } catch (std::invalid_argument &e) {
    std::cerr << "Cannot instantiate WMS\n";
    exit(1);
  }

  // Create the Workflow
  Workflow *workflow = nullptr;
  try {
    workflow = createWorkflow(argv[3]);
  } catch (std::invalid_argument &e) {
    std::cerr << "Cannot create workflow: " << e.what() << "\n";
  }
  wms->addWorkflow(workflow, workflow_start_time);

  // Launch the simulation
  try {
    std::cerr << "Launching simulation!\n";
    simulation->launch();
  } catch (std::runtime_error &e) {
    std::cerr << "Simulation failed: " << e.what() << "\n";
    exit(1);
  }

  std::cerr << "Done!\n";

}

void setupSimulationPlatform(Simulation *simulation, int num_compute_nodes) {

  // Create a the platform file
  std::string xml = "<?xml version='1.0'?>\n"
          "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd\">\n"
          "<platform version=\"4.1\">\n"
          "   <zone id=\"AS0\" routing=\"Full\"> "
          "       <host id=\"Login\" speed=\"1f\" core=\"2\"/>\n";
  for (int i=0; i < num_compute_nodes; i++) {
    xml += "       <host id=\"ComputeNode_"+std::to_string(i)+"\" speed=\"1f\" core=\"1\"/>\n";
  }
  xml +=        "        <link id=\"1\" bandwidth=\"5000GBps\" latency=\"0us\"/>\n";
  for (int i=0; i < num_compute_nodes; i++) {
    xml += "       <route src=\"Login\" dst=\"ComputeNode_"+std::to_string(i)+"\"> <link_ctn id=\"1\"/> </route>\n";
  }
  for (int i=0; i < num_compute_nodes; i++) {
    for (int j = i+1; j < num_compute_nodes; j++) {
      xml += "       <route src=\"ComputeNode_" + std::to_string(i) + "\" dst=\"ComputeNode_" + std::to_string(j) + "\"> <link_ctn id=\"1\"/> </route>\n";
    }
  }
  xml += "   </zone>\n";
  xml += "</platform>\n";

  FILE *platform_file = fopen("/tmp/platform.xml", "w");
  fprintf(platform_file, "%s", xml.c_str());
  fclose(platform_file);

  try {
    simulation->instantiatePlatform("/tmp/platform.xml");
  } catch (std::invalid_argument &e) {  // Unfortunately S4U doesn't throw for this...
    throw std::runtime_error("Invalid generated XML platform file");
  }
}

Workflow *createWorkflow(std::string workflow_spec) {

  std::istringstream ss(workflow_spec);
  std::string token;
  std::vector<std::string> tokens;

  while(std::getline(ss, token, ':')) {
    tokens.push_back(token);
  }

  if (tokens[0] == "indep") {
    if (tokens.size() != 4) {
      throw std::invalid_argument("createWorkflow(): Invalid workflow specification " + workflow_spec);
    }
    try {
      return createIndepWorkflow(tokens);
    } catch (std::invalid_argument &e) {
      throw;
    }
  } else {
    throw std::invalid_argument("createWorkflow(): Unknown workflow type " + tokens[0]);
  }
}

Workflow *createIndepWorkflow(std::vector<std::string> spec_tokens) {
  unsigned long num_tasks;
  unsigned long min_time;
  unsigned long max_time;

  std::default_random_engine rng;

  if ((sscanf(spec_tokens[1].c_str(), "%lu", &num_tasks) != 1) or (num_tasks < 1)) {
    throw std::invalid_argument("createIndepWorkflow(): invalid number of tasks in indep workflow specification");
  }
  if ((sscanf(spec_tokens[2].c_str(), "%lu", &min_time) != 1) or (min_time < 0.0)) {
    throw std::invalid_argument("createIndepWorkflow(): invalid min task exec time in indep workflow specification");
  }
  if ((sscanf(spec_tokens[3].c_str(), "%lu", &max_time) != 1) or (max_time < 0.0) or (max_time < min_time)) {
    throw std::invalid_argument("createIndepWorkflow(): invalid max task exec time in indep workflow specification");
  }

  auto workflow = new Workflow();

  static std::uniform_int_distribution<unsigned long> m_udist(min_time, max_time);
  for (int i=0; i < num_tasks; i++) {
    unsigned long flops = m_udist(rng);
    std::cerr << "FLOPS = " << flops << "\n";
    workflow->addTask("Task_" + std::to_string(i), flops, 1, 1, 1.0, 0.0);
  }

  return workflow;

}


StandardJobScheduler *createStandardJobScheduler(std::string scheduler_spec) {

  std::istringstream ss(scheduler_spec);
  std::string token;
  std::vector<std::string> tokens;

  while(std::getline(ss, token, ':')) {
    tokens.push_back(token);
  }

  if (tokens[0] == "fixed_clustering") {
    if (tokens.size() != 2) {
      throw std::invalid_argument("createStandardJobScheduler(): Invalid fixed_clustering specification");
    }
    int num_tasks_per_cluster;
    if ((sscanf(tokens[1].c_str(), "%d", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1)) {
      throw std::invalid_argument("createStandardJobScheduler(): Invalid fixed_clustering specification");
    }
    return new FixedClusteringScheduler(num_tasks_per_cluster);
  } else {
    throw std::invalid_argument("createStandardJobScheduler(): Unknown workflow type " + tokens[0]);
  }

}
