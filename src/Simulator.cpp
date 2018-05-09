
#include <iostream>
#include <wrench-dev.h>
#include <services/compute/batch/BatchServiceProperty.h>
#include "FixedClusteringAlgorithms/FixedClusteringWMS.h"
#include "FixedClusteringAlgorithms/FixedClusteringScheduler.h"
#include "ZhangClusteringAlgorithm/ZhangClusteringWMS.h"

using namespace wrench;

void setupSimulationPlatform(Simulation *simulation, unsigned long num_compute_nodes);
Workflow *createWorkflow(std::string workflow_spec);
Workflow *createIndepWorkflow(std::vector<std::string> spec_tokens);
Workflow *createLevelsWorkflow(std::vector<std::string> spec_tokens);
WMS *createWMS(std::string scheduler_spec, unsigned long max_num_jobs, std::string algorithm_name, BatchService *batch_service);

int main(int argc, char **argv) {

  // Create and initialize a simulation
  auto simulation = new wrench::Simulation();
  simulation->init(&argc, argv);

  // Parse command-line arguments
  if (argc != 7) {
    std::cerr << "Usage: " << argv[0] << " <num_compute_nodes> <SWF job trace file> <workflow specification> <workflow start time> <max num concurrent jobs> <algorithm>" << "\n";
    std::cerr << "  * workflow specification options:" << "\n";
    std::cerr << "    - indep:n:t1:t2 " << "\n";
    std::cerr << "      - n: number of tasks" << "\n";
    std::cerr << "      - t1/t2: min/max task durations in integral second (uniformly distributed)" << "\n";
    std::cerr << "      (just a set of independent tasks)" << "\n";
    std::cerr << "    - levels:l0:l2:....:ln:t1:t2" << "\n";
    std::cerr << "      - lx: num tasks in level x" << "\n";
    std::cerr << "      - t1/t2: min/max task durations in integral second (uniformly distributed)" << "\n";
    std::cerr << "  * algorithm  options:" << "\n";
    std::cerr << "    - fixed_clustering:n:m" << "\n";
    std::cerr << "      - n: number of tasks in each cluster" << "\n";
    std::cerr << "      - m: number of nodes used to execute each cluster" << "\n";
    std::cerr << "      (ready tasks are grouped into clustered \"arbitrarily\"\n";
    std::cerr << "    - zhang (algorithm by Zhang, Koelbel, Cooper)" << "\n";
    exit(1);
  }
  unsigned long num_compute_nodes;
  if ((sscanf(argv[1], "%lu", &num_compute_nodes) != 1) or (num_compute_nodes < 1)) {
    std::cerr << "Invalid number of compute nodes\n";
  }

  double workflow_start_time;
  if ((sscanf(argv[4], "%lf", &workflow_start_time) != 1) or (workflow_start_time < 0)) {
    std::cerr << "Invalid workflow start time\n";
  }

  unsigned long max_num_jobs;
  if ((sscanf(argv[5], "%lu", &max_num_jobs) != 1) or (max_num_jobs < 1)) {
    std::cerr << "Invalid maximum number of concurrent jobs\n";
  }

  std::string scheduler_spec = argv[6];

  // Setup the simulation platform
  setupSimulationPlatform(simulation, num_compute_nodes);

  // Create a BatchService
  std::vector<std::string> compute_nodes;
  for (int i=0; i < num_compute_nodes; i++) {
    compute_nodes.push_back("ComputeNode_" + std::to_string(i));
  }
  BatchService *batch_service;
  std::string login_hostname = "Login";
  try {
    batch_service = new BatchService(login_hostname, true, true, compute_nodes, nullptr,
                                     {{BatchServiceProperty::BATCH_SCHEDULING_ALGORITHM, "conservative_bf"},
                                      {BatchServiceProperty::SIMULATED_WORKLOAD_TRACE_FILE, argv[2]}
                                     });
  } catch (std::invalid_argument &e) {
    std::cerr << "Cannot instantiate batch service: " << e.what() << "\n";
    std::cerr << "Trying the non-BATSCHED option...\n";
    try {
      batch_service = new BatchService(login_hostname, true, true, compute_nodes, nullptr,
                                       {{BatchServiceProperty::BATCH_SCHEDULING_ALGORITHM,    "FCFS"},
                                        {BatchServiceProperty::SIMULATED_WORKLOAD_TRACE_FILE, argv[2]}
                                       });
    } catch (std::invalid_argument &e) {
      std::cerr << "Cannot instantiate batch service: " << e.what() << "\n";
      std::cerr << "Giving up\n";
      exit(1);
    }
  }

  simulation->add(batch_service);

  // Create the WMS
  WMS *wms = nullptr;
  try {
    wms = createWMS("Login", max_num_jobs, scheduler_spec, batch_service);
  } catch (std::invalid_argument &e) {
    std::cerr << "Cannot instantiate WMS: " << e.what() << "\n";
    exit(1);
  }

  try {
    simulation->add(wms);
  } catch (std::invalid_argument &e) {
    std::cerr << "Cannot add WMS to simulation: " << e.what() << "\n";
    exit(1);
  }


  // Create the Workflow
  Workflow *workflow = nullptr;
  try {
    workflow = createWorkflow(argv[3]);
  } catch (std::invalid_argument &e) {
    std::cerr << "Cannot create workflow: " << e.what() << "\n";
    exit(1);
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

void setupSimulationPlatform(Simulation *simulation, unsigned long num_compute_nodes) {

  // Create a the platform file
  std::string xml = "<?xml version='1.0'?>\n"
          "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd\">\n"
          "<platform version=\"4.1\">\n"
          "   <zone id=\"AS0\" routing=\"Full\"> "
          "       <host id=\"Login\" speed=\"1f\" core=\"2\"/>\n";
  for (unsigned long i=0; i < num_compute_nodes; i++) {
    xml += "       <host id=\"ComputeNode_"+std::to_string(i)+"\" speed=\"1f\" core=\"1\"/>\n";
  }
  xml +=        "        <link id=\"1\" bandwidth=\"5000GBps\" latency=\"0us\"/>\n";
  for (unsigned long i=0; i < num_compute_nodes; i++) {
    xml += "       <route src=\"Login\" dst=\"ComputeNode_"+std::to_string(i)+"\"> <link_ctn id=\"1\"/> </route>\n";
  }
  for (unsigned long i=0; i < num_compute_nodes; i++) {
    for (unsigned long j = i+1; j < num_compute_nodes; j++) {
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

  } else if (tokens[0] == "levels") {
    if (tokens.size() < 4) {
      throw std::invalid_argument("createWorkflow(): Invalid workflow specification " + workflow_spec);
    }
    try {
      return createLevelsWorkflow(tokens);
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
    throw std::invalid_argument("createIndepWorkflow(): invalid number of tasks in workflow specification");
  }
  if ((sscanf(spec_tokens[2].c_str(), "%lu", &min_time) != 1) or (min_time < 0.0)) {
    throw std::invalid_argument("createIndepWorkflow(): invalid min task exec time in workflow specification");
  }
  if ((sscanf(spec_tokens[3].c_str(), "%lu", &max_time) != 1) or (max_time < 0.0) or (max_time < min_time)) {
    throw std::invalid_argument("createIndepWorkflow(): invalid max task exec time in workflow specification");
  }

  auto workflow = new Workflow();

  static std::uniform_int_distribution<unsigned long> m_udist(min_time, max_time);
  for (unsigned long i=0; i < num_tasks; i++) {
    unsigned long flops = m_udist(rng);
    workflow->addTask("Task_" + std::to_string(i), (double)flops, 1, 1, 1.0, 0.0);
  }

  return workflow;

}


Workflow *createLevelsWorkflow(std::vector<std::string> spec_tokens) {

  unsigned long min_time;
  unsigned long max_time;
  std::default_random_engine rng;

  unsigned long num_levels = spec_tokens.size()-3;

  unsigned long num_tasks[num_levels];

  std::cerr << "Creating a 'levels' workflow...\n";

  for (unsigned long l = 0; l < num_levels; l++) {
    if ((sscanf(spec_tokens[l+1].c_str(), "%lu", &(num_tasks[l])) != 1) or (num_tasks[l] < 1)) {
      throw std::invalid_argument("createLevelsWorkflow(): invalid number of tasks in level " + std::to_string(l) + " workflow specification");
    }
  }

  if ((sscanf(spec_tokens[spec_tokens.size()-2].c_str(), "%lu", &min_time) != 1) or (min_time < 0.0)) {
    throw std::invalid_argument("createLevelsWorkflow(): invalid min task exec time in workflow specification");
  }
  if ((sscanf(spec_tokens[spec_tokens.size()-1].c_str(), "%lu", &max_time) != 1) or (max_time < 0.0) or (max_time < min_time)) {
    throw std::invalid_argument("createLevelsWorkflow(): invalid max task exec time in workflow specification");
  }

  auto workflow = new Workflow();

  // Create the tasks
  std::vector<wrench::WorkflowTask *> tasks[num_levels];

  static std::uniform_int_distribution<unsigned long> m_udist(min_time, max_time);
  for (unsigned long l=0; l < num_levels; l++) {
    for (unsigned long t=0; t < num_tasks[l]; t++) {
      unsigned long flops = m_udist(rng);
      tasks[l].push_back(workflow->addTask("Task_l" + std::to_string(l) + "_" + std::to_string(t), (double) flops, 1, 1, 1.0, 0.0));
    }
  }

  // Create the control dependencies (right now FULL dependencies)
  for (unsigned long l=1; l < num_levels; l++) {
    for (unsigned long t=0; t < num_tasks[l]; t++) {
      for (unsigned long p=0; p < num_tasks[l-1]; p++) {
        workflow->addControlDependency(tasks[l-1][p], tasks[l][t]);
      }
    }
  }

  // Make sure we have what we think:
  for (auto t : workflow->getTasks()) {
    std::cerr << "Task " << t->getId() << " has  has " << t->getNumberOfChildren() << " children\n";
  }

//  workflow->exportToEPS("/tmp/foo.eps");

  return workflow;

}


WMS *createWMS(std::string hostname,
                                unsigned long max_num_jobs,
                                std::string scheduler_spec,
                                BatchService *batch_service) {

  std::istringstream ss(scheduler_spec);
  std::string token;
  std::vector<std::string> tokens;

  while(std::getline(ss, token, ':')) {
    tokens.push_back(token);
  }


  if (tokens[0] == "fixed_clustering") {
    if (tokens.size() != 3) {
      throw std::invalid_argument("createStandardJobScheduler(): Invalid fixed_clustering specification");
    }
    unsigned long num_tasks_per_cluster;
    unsigned long num_nodes_per_cluster;
    if ((sscanf(tokens[1].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
        (sscanf(tokens[2].c_str(), "%lu", &num_nodes_per_cluster) != 1) or (num_nodes_per_cluster < 1)) {
      throw std::invalid_argument("createStandardJobScheduler(): Invalid fixed_clustering specification");
    }
    WMS *wms = new FixedClusteringWMS(hostname, new FixedClusteringScheduler(num_tasks_per_cluster, num_nodes_per_cluster,
                                                                         max_num_jobs), batch_service);
    return wms;

  } else if (tokens[0] == "zhang") {

    return new ZhangClusteringWMS(hostname, batch_service);

  } else {
    throw std::invalid_argument("createStandardJobScheduler(): Unknown workflow type " + tokens[0]);
  }

}
