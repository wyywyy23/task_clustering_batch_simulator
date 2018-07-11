
#include <iostream>
#include <wrench-dev.h>
#include <services/compute/batch/BatchServiceProperty.h>
#include <OneJobClusteringAlgorithm/OneJobClusteringWMS.h>
#include "StaticClusteringAlgorithms/StaticClusteringWMS.h"
#include "ZhangClusteringAlgorithm/ZhangClusteringWMS.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(task_clustering_simulator, "Log category for Task Clustering Simulator");


using namespace wrench;

void setupSimulationPlatform(Simulation *simulation, unsigned long num_compute_nodes);
Workflow *createWorkflow(std::string workflow_spec);
Workflow *createIndepWorkflow(std::vector<std::string> spec_tokens);
Workflow *createLevelsWorkflow(std::vector<std::string> spec_tokens);
Workflow *createDAXWorkflow(std::vector<std::string> spec_tokens);
WMS *createWMS(std::string scheduler_spec, BatchService *batch_service, unsigned long max_num_jobs, std::string algorithm_name);


int main(int argc, char **argv) {

  // Create and initialize a simulation
  auto simulation = new wrench::Simulation();
  simulation->init(&argc, argv);

  // Parse command-line arguments
  if ((argc != 8) and (argc != 9)) {
    std::cerr << "\e[1;31mUsage: " << argv[0] << " <num_compute_nodes> <SWF job trace file> <max jobs in system> <workflow specification> <workflow start time> <algorithm> <batch algorithm> [csv batch log file]\e[0m" << "\n";
    std::cerr << "  \e[1;32m### workflow specification options ###\e[0m" << "\n";
    std::cerr << "    *  \e[1mindep:s:n:t1:t2\e[0m " << "\n";
    std::cerr << "      - Just a set of independent tasks" << "\n";
    std::cerr << "      - n: number of tasks" << "\n";
    std::cerr << "      - t1/t2: min/max task durations in integral seconds (actual times uniformly sampled)" << "\n";
    std::cerr << "      - s: rng seed" << "\n";
    std::cerr << "    * \e[1mlevels:s:l0:t0:T0:l1:t1:T1:....:ln:tn:Tn\e[0m" << "\n";
    std::cerr << "      - A strictly levelled workflow" << "\n";
    std::cerr << "      - lx: num tasks in level x" << "\n";
    std::cerr << "      - each task in level x depends on ALL tasks in level x-1" << "\n";
    std::cerr << "      - tx/Tx: min/max task durations in level x, in integral second (times uniformly sampled)" << "\n";
    std::cerr << "      - s: rng seed" << "\n";
    std::cerr << "    * \e[1mldax:filename\e[0m" << "\n";
    std::cerr << "      - A workflow imported from a DAX file" << "\n";
    std::cerr << "      - Files and Data dependencies are ignored. Only control dependencies are preserved" << "\n";
    std::cerr << "\n";
    std::cerr << "  \e[1;32m### algorithm options ###\e[0m" << "\n";
    std::cerr << "    * \e[1mstatic:one_job-m\e[0m" << "\n";
    std::cerr << "      - run the workflow as a single job" << "\n";
    std::cerr << "      - pick the job size (num of hosts) based on a estimation of the queue waiting time" << "\n";
    std::cerr << "        and the makespan (estimated assuming some arbitrary list-scheduling heuristic)" << "\n";
    std::cerr << "      - m: number of hosts used to execute the job" << "\n";
    std::cerr << "    * \e[1mstatic:one_job_per_task\e[0m" << "\n";
    std::cerr << "      - run each task as a single one-host job" << "\n";
    std::cerr << "      - Submit a job only once a task is ready, no queue wait time estimation" << "\n";
    std::cerr << "    * \e[1mstatic:hc-[vprior|vposterior|vnone]-n-m\e[0m" << "\n";
    std::cerr << "      - Horizontal Clustering algorithm (ref [12] in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al.)" << "\n";
    std::cerr << "      - Simply clusters tasks in each level in whatever order and execute " << "\n";
    std::cerr << "        each cluster on the same number of hosts " << "\n";
    std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
    std::cerr << "      - n: number of ready tasks in each cluster" << "\n";
    std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
    std::cerr << "    * \e[1mstatic:dfjs-[vprior|vposterior|vnone]-t-m\e[0m" << "\n";
    std::cerr << "      - DFJS algorithm (ref [5] in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al.)" << "\n";
    std::cerr << "      - Simply greedily clusters tasks in each level in whatever order so that " << "\n";
    std::cerr << "        each cluster has a runtime that's bounded. (extended to deal with numbers of hosts)" << "\n";
    std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
    std::cerr << "      - t: bound on runtime (in seconds)" << "\n";
    std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
    std::cerr << "    * \e[1mstatic:hrb-[vprior|vposterior|vnone]-n-m\e[0m" << "\n";
    std::cerr << "      - The HRB algorithm in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al." << "\n";
    std::cerr << "      - Modified to specify 'number of tasks per cluster' rather than 'number of clusters per level'" << "\n";
    std::cerr << "      - Cluster tasks in each level so that clusters are load-balanced" << "\n";
    std::cerr << "        (extended to deal with numbers of hosts)" << "\n";
    std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
    std::cerr << "      - n: number of ready tasks in each cluster" << "\n";
    std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
    std::cerr << "    * \e[1mstatic:hifb-[vprior|vposterior|vnone-]n-m\e[0m" << "\n";
    std::cerr << "      - The HIFB algorithm in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al." << "\n";
    std::cerr << "      - Modified to specify 'number of tasks per cluster' rather than 'number of clusters per level'" << "\n";
    std::cerr << "      - Cluster tasks in each level so that clusters have similar impact factors" << "\n";
    std::cerr << "        (extended to deal with numbers of hosts)" << "\n";
    std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
    std::cerr << "      - n: number of ready tasks in each cluster" << "\n";
    std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
    std::cerr << "    * \e[1mstatic:hdb-[vprior|vposterior|vnone]-n-m\e[0m" << "\n";
    std::cerr << "      - The HDB algorithm in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al." << "\n";
    std::cerr << "      - Modified to specify 'number of tasks per cluster' rather than 'number of clusters per level'" << "\n";
    std::cerr << "      - Cluster tasks in each level so that clusters have similar distances" << "\n";
    std::cerr << "        (extended to deal with numbers of hosts)" << "\n";
    std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
    std::cerr << "      - n: number of ready tasks in each cluster" << "\n";
    std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
    std::cerr << "    * \e[1mstatic:vc\e[0m" << "\n";
    std::cerr << "      - The VC algorithm in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al." << "\n";
    std::cerr << "      - Cluster tasks with single-parent-single-child depepdencies" << "\n";
    std::cerr << "    * \e[1mzhang:[overlap|nooverlap]:[plimit|pnolimit]\e[0m" << "\n";
    std::cerr << "      - The algorithm by Zhang, Koelbel, and Cooper" << "\n";
    std::cerr << "      - [overlap|nooverlap]: use the default 'overlap' behavior by which a pilot job" << "\n";
    std::cerr << "        is always queued while another is running. Specify 'nooverlap' disables this," << "\n";
    std::cerr << "        which is useful for quantifying how much overlapping helps" << "\n";
    std::cerr << "      - [plimit|pnolimit]: plimit is the original algorithm that will complain if the workflow" << "\n";
    std::cerr << "        parallelism is larger than the number of hosts. pnolimit is an extension that will not" << "\n";
    std::cerr << "        complain and just fold a level, useful to use the zhang algorithm for more cases, although" << "\n";
    std::cerr << "        not intended by its authors. Also, pnolimit uses the smallest, best number of hosts" << "\n";
    std::cerr << "        to pack that tasks into a job" << "\n";
    std::cerr << "\n";
    std::cerr << "  \e[1;32m### batch algorithm options ###\e[0m" << "\n";
    std::cerr << "    * \e[1mconservative_bf\e[0m" << "\n";
    std::cerr << "      - classical conservative backfilling" << "\n";
    std::cerr << "    * \e[1mfast_conservative_bf\e[0m" << "\n";
    std::cerr << "      - faster backfilling algorithm, still conservative but not exactly canon" << "\n";
    std::cerr << "    * \e[1mfcfs_fast\e[0m" << "\n";
    std::cerr << "      - first come, first serve" << "\n";
    std::cerr << "\n";
    exit(1);
  }
  unsigned long num_compute_nodes;
  if ((sscanf(argv[1], "%lu", &num_compute_nodes) != 1) or (num_compute_nodes < 1)) {
    std::cerr << "Invalid number of compute nodes\n";
  }

  unsigned long max_num_jobs;
  if ((sscanf(argv[3], "%lu", &max_num_jobs) != 1) or (max_num_jobs < 1)) {
    std::cerr << "Invalid maximum number of jobs\n";
  }


  double workflow_start_time;
  if ((sscanf(argv[5], "%lf", &workflow_start_time) != 1) or (workflow_start_time < 0)) {
    std::cerr << "Invalid workflow start time\n";
  }

  std::string scheduler_spec = std::string(argv[6]);

  // Setup the simulation platform
  setupSimulationPlatform(simulation, num_compute_nodes);

  // Create a BatchService
  std::vector<std::string> compute_nodes;
  for (unsigned int i=0; i < num_compute_nodes; i++) {
    compute_nodes.push_back(std::string("ComputeNode_") + std::to_string(i));
  }
  BatchService *batch_service;
  std::string login_hostname = "Login";

  std::string csv_batch_log = "/tmp/batch_log.csv";
  if (argc == 9) {
    csv_batch_log = std::string(argv[8]);
  }

  try {
    batch_service = new BatchService(login_hostname, compute_nodes, 0,
                                     {{BatchServiceProperty::OUTPUT_CSV_JOB_LOG, csv_batch_log},
                                      {BatchServiceProperty::BATCH_SCHEDULING_ALGORITHM, std::string(argv[7])},
                                      {BatchServiceProperty::SIMULATED_WORKLOAD_TRACE_FILE, std::string(argv[2])}
                                     }, {});
  } catch (std::invalid_argument &e) {

    WRENCH_INFO("Cannot instantiate batch service: %s", e.what());
    WRENCH_INFO("Trying the non-BATSCHED option...");
    try {
      batch_service = new BatchService(login_hostname, compute_nodes, 0,
                                       {{BatchServiceProperty::BATCH_SCHEDULING_ALGORITHM,    "FCFS"},
                                        {BatchServiceProperty::SIMULATED_WORKLOAD_TRACE_FILE, std::string(argv[2])}
                                       }, {});
    } catch (std::invalid_argument &e) {
      WRENCH_INFO("Cannot instantiate batch service: %s", e.what());
      std::cerr << "Giving up as I cannot instantiate the Batch Service\n";
      exit(1);
    }
  }

  simulation->add(batch_service);

  // Create the WMS
  WMS *wms = nullptr;
  try {
    wms = createWMS("Login", batch_service, max_num_jobs, scheduler_spec);
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
    workflow = createWorkflow(argv[4]);
  } catch (std::invalid_argument &e) {
    std::cerr << "Cannot create workflow: " << e.what() << "\n";
    exit(1);
  }
  wms->addWorkflow(workflow, workflow_start_time);

  // Launch the simulation
  try {
    WRENCH_INFO("Launching simulation!");
    simulation->launch();
  } catch (std::runtime_error &e) {
    std::cerr << "Simulation failed: " << e.what() << "\n";
    exit(1);
  }
  WRENCH_INFO("Simulation done!");


  std::cout << "WORKFLOW MAKESPAN: " << (workflow->getCompletionDate() - workflow_start_time) << "\n";
  std::cerr << "CSV Log file create at " << csv_batch_log << "\n";

}

void setupSimulationPlatform(Simulation *simulation, unsigned long num_compute_nodes) {

  // Create a the platform file
  std::string xml = "<?xml version='1.0'?>\n"
          "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd\">\n"
          "<platform version=\"4.1\">\n"
          "   <zone id=\"AS0\" routing=\"Full\"> "
          "       <host id=\"Login\" speed=\"1f\" core=\"1\"/>\n";
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
    if (tokens.size() != 5) {
      throw std::invalid_argument("createWorkflow(): Invalid workflow specification " + workflow_spec);
    }
    try {
      return createIndepWorkflow(tokens);
    } catch (std::invalid_argument &e) {
      throw;
    }

  } else if (tokens[0] == "levels") {
    if ((tokens.size() == 2) or (tokens.size() - 2) % 3) {
      throw std::invalid_argument("createWorkflow(): Invalid workflow specification " + workflow_spec);
    }
    try {
      return createLevelsWorkflow(tokens);
    } catch (std::invalid_argument &e) {
      throw;
    }

  } else if (tokens[0] == "dax") {
    if (tokens.size() != 2) {
      throw std::invalid_argument("createWorkflow(): Invalid workflow specification " + workflow_spec);
    }
    try {
      return createDAXWorkflow(tokens);
    } catch (std::invalid_argument &e) {
      throw;
    }

  } else {
    throw std::invalid_argument("createWorkflow(): Unknown workflow type " + tokens[0]);
  }
}

Workflow *createIndepWorkflow(std::vector<std::string> spec_tokens) {
  unsigned int seed;
  if (sscanf(spec_tokens[1].c_str(), "%u", &seed) != 1) {
    throw std::invalid_argument("createIndepWorkflow(): invalid RNG ssed in workflow specification");
  }
  std::default_random_engine rng(seed);

  unsigned long num_tasks;
  unsigned long min_time;
  unsigned long max_time;

  if ((sscanf(spec_tokens[2].c_str(), "%lu", &num_tasks) != 1) or (num_tasks < 1)) {
    throw std::invalid_argument("createIndepWorkflow(): invalid number of tasks in workflow specification");
  }
  if ((sscanf(spec_tokens[3].c_str(), "%lu", &min_time) != 1) or (min_time < 0.0)) {
    throw std::invalid_argument("createIndepWorkflow(): invalid min task exec time in workflow specification");
  }
  if ((sscanf(spec_tokens[4].c_str(), "%lu", &max_time) != 1) or (max_time < 0.0) or (max_time < min_time)) {
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

  unsigned int seed;
  if (sscanf(spec_tokens[1].c_str(), "%u", &seed) != 1) {
    throw std::invalid_argument("createLevelsWorkflow(): invalid RNG ssed in workflow specification");
  }
  std::default_random_engine rng(seed);

  unsigned long num_levels = (spec_tokens.size()-1)/3;

  unsigned long num_tasks[num_levels];
  unsigned long min_times[num_levels];
  unsigned long max_times[num_levels];

  WRENCH_INFO("Creating a 'levels' workflow...");

  for (unsigned long l = 0; l < num_levels; l++) {
    if ((sscanf(spec_tokens[2+l*3].c_str(), "%lu", &(num_tasks[l])) != 1) or (num_tasks[l] < 1)) {
      throw std::invalid_argument("createLevelsWorkflow(): invalid number of tasks in level " + std::to_string(l) + " workflow specification");
    }

    if ((sscanf(spec_tokens[2+l*3+1].c_str(), "%lu", &(min_times[l])) != 1)) {
      throw std::invalid_argument("createLevelsWorkflow(): invalid min task exec time in workflow specification");
    }
    if ((sscanf(spec_tokens[2+l*3+2].c_str(), "%lu", &(max_times[l])) != 1) or (max_times[l] < min_times[l])) {
      throw std::invalid_argument("createLevelsWorkflow(): invalid max task exec time in workflow specification");
    }
  }

  auto workflow = new Workflow();

  // Create the tasks
  std::vector<wrench::WorkflowTask *> tasks[num_levels];

  std::uniform_int_distribution<unsigned long> *m_udists[num_levels];
  for (unsigned long l=0; l < num_levels; l++) {
    m_udists[l] = new std::uniform_int_distribution<unsigned long>(min_times[l], max_times[l]);
  }

  for (unsigned long l=0; l < num_levels; l++) {
    for (unsigned long t=0; t < num_tasks[l]; t++) {
      unsigned long flops = (*m_udists[l])(rng);
      tasks[l].push_back(workflow->addTask("Task_l" + std::to_string(l) + "_" +
                                                   std::to_string(t), (double) flops, 1, 1, 1.0, 0.0));
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

//  workflow->exportToEPS("/tmp/foo.eps");

  return workflow;

}

Workflow *createDAXWorkflow(std::vector<std::string> spec_tokens) {
  std::string filename = spec_tokens[1];

  auto original_workflow = new Workflow();
  try {
    original_workflow->loadFromDAX(filename, "1");
  } catch (std::invalid_argument &e) {
    throw std::runtime_error("Cannot import workflow from DAX");
  }

  auto workflow = new Workflow();

  // Add task replicas
  for (auto t : original_workflow->getTasks()) {
//    WRENCH_INFO("t->getFlops() = %lf", t->getFlops());
    workflow->addTask(t->getID(), t->getFlops(), 1, 1, 1.0, 0);
  }

  // Deal with all dependencies (brute-force, but whatever)
  for (auto t : original_workflow->getTasks()) {
    std::vector<wrench::WorkflowTask *> parents = original_workflow->getTaskParents(t);
    std::vector<wrench::WorkflowTask *> children = original_workflow->getTaskChildren(t);

    for (auto p : parents) {
      std::string parent_id = p->getID();
      std::string child_id = t->getID();
      workflow->addControlDependency(workflow->getTaskByID(parent_id), workflow->getTaskByID(child_id));
    }
    for (auto c : children) {
      std::string parent_id = t->getID();
      std::string child_id = c->getID();
      workflow->addControlDependency(workflow->getTaskByID(parent_id), workflow->getTaskByID(child_id));
    }
  }
//  WRENCH_INFO("NEW WORKFLOW HAS %ld TASKS and %ld LEVELS", workflow->getNumberOfTasks(), workflow->getNumLevels());

  return workflow;

}


WMS *createWMS(std::string hostname,
               BatchService *batch_service,
               unsigned long max_num_jobs,
               std::string scheduler_spec) {

  std::istringstream ss(scheduler_spec);
  std::string token;
  std::vector<std::string> tokens;

  while(std::getline(ss, token, ':')) {
    tokens.push_back(token);
  }

  if (tokens[0] == "static") {

    if (tokens.size() != 2) {
      throw std::invalid_argument("createStandardJobScheduler(): Invalid static specification");
    }

    WMS *wms = new StaticClusteringWMS(hostname, batch_service, max_num_jobs, tokens[1]);
    return wms;

  } else if (tokens[0] == "zhang") {

    if (tokens.size() != 3) {
      throw std::invalid_argument("createStandardJobScheduler(): Invalid zhang specification");
    }
    bool overlap;
    if (tokens[1] == "overlap") {
      overlap = true;
    } else if (tokens[1] == "nooverlap") {
      overlap = false;
    } else {
      throw std::invalid_argument("createStandardJobScheduler(): Invalid zhang specification");
    }
    bool plimit;
    if (tokens[2] == "plimit") {
      plimit = true;
    } else if (tokens[2] == "pnolimit") {
      plimit = false;
    } else {
      throw std::invalid_argument("createStandardJobScheduler(): Invalid zhang specification");
    }
    return new ZhangClusteringWMS(hostname, overlap, plimit, batch_service);

  } else {
    throw std::invalid_argument("createStandardJobScheduler(): Unknown workflow type " + tokens[0]);
  }

}
