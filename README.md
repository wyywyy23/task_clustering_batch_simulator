## Task Clustering Algorithm Simulator

This is a WRENCH-based simulator designed for exploring task clustering algorithms in  the context of executing a workflow application on a batch-scheduled cluster.

## Prerequisites

This project is fully developed in C++. The code follows the C++11 standard, and thus older 
compilers tend to fail the compilation process. Therefore, we strongly recommend
users to satisfy the following requirements:

- **CMake** - version 3.5 or higher
  
And, one of the following:
- **g++** - version 5.0 or higher
- **clang** - version 3.6 or higher

## Dependencies

- [Batsched](https://gitlab.inria.fr/batsim/batsched)
- [WRENCH](https://github.com/wrench-project/wrench) - Built with  ```cmake -DENABLE_BATSCHED=on```

## Installation

Provided Batsched and WRENCH have been installed, the simulator is
compiled and installed as:

```bash
cmake .
make
sudo make install
```

## Usage

```bash
$ simulator <compute_nodes> <trace_file> <max_jobs> <workflow_specification> <start_time> <algorithm> <batch_algorithm> [<logging_option>] <output_file>]
```

  - ```<compute_nodes>```: the number of compute nodes in the cluster;
  - ```<trace_file>```: the path of a workload trace file that defines the dynamic "background" load on the cluster. Workflow execution competes with this background load for access to compute nodes;
  - ```<max_jobs>```: the maximum number of jobs allowed in the system per user onthe cluster (which thus limits the number of workflow jobs  that can be in the system at  a given time;
  - ```<workflow_specification>```: A workflow description file that gives workflow task execution times and dependencies (in [DAX format](https://pegasus.isi.edu/documentation/creating_workflows.php));
  - ```<start_time>```: the date at which the workflow execution begins (i.e., when the first task can be submitted for execution);
  - ```<algorithm>```: the application-level workflow scheduling algorithm to employ;
  - ```<batch_algorithm>```: the batch scheduling algorithm used by the batch scheduler on the cluster;
  - ```<logging_option>```: an optional argument, available to all WRENCH-based simulators, to configure the level of simulation logging;
  - ```<output_file>```: and optional argument that is a path to a json file to which simulation output should be written.

Invoking the simulator with no arguments outputs a long and detailed usage description, which, in particular, details all available ```<algorithm>``` argument values (redacted output):

```
Usage: ./simulator <num_compute_nodes> <job trace file> <max jobs in system> <workflow specification> <workflow start time> <algorithm> <batch algorithm> [DISABLED: csv batch log file] [json result file]
  ### workflow specification options ###
	...
    * dax:filename
      - A workflow imported from a DAX file
      - Files and Data dependencies are ignored. Only control dependencies are preserved

  ### algorithm options ###
    * static:levelbylevel-m
      - run each workflow level as a job
      - m: number of hosts used to execute each job
              - if m = 0, then pick best number nodes based on queue wait time prediction
    * static:one_job-m-waste_bound
      - run the workflow as a single job
      - m: number of hosts used to execute the job
              - if m = 0, then pick best number nodes based on queue wait time prediction
      - waste_bound: maximum percentage of wasted node time e.g. 0.2
    * static:one_job_per_task
      - run each task as a single one-host job
      - Submit a job only once a task is ready, no queue wait time estimation
	...

  ### batch algorithm options ###
    * conservative_bf
      - classical conservative backfilling
    ...

```

 
## Example invocations
```bash
$ simulator 338 NASA-iPSC-1993-3.swf 16 dax:CYBERSHAKE_50_360000.dax 0 glume:1:0 conservative_bf --wrench-no-log

$ simulator 10 NASA-iPSC-1993-3.swf 10 levels:42:10:10:1000 0 zhang:global:bsearch:prediction conservative_bf --log=root.threshold:critical --log=zhang_clustering_wms.threshold=info
  
$ simulator 100 kth_sp2.swf 8 levels:42:10:10:1000 600000 static:one_job-0-1 conservative_bf out.json
```
