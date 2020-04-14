## Task Clustering Algorithm Simulator

This is a WRENCH-based simulator designed for exploring task clustering algorithms. Specifically, this simulates the scheduling and execution of workflows on batch-scheduled clusters.

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
- [WRENCH](https://github.com/wrench-project/wrench) - Build with  ```cmake -DENABLE_BATSCHED=on```

## Installation

After installing batsched and WRENCH, compiling and installing with:

```bash
cmake .
make
sudo make install
```

## Usage

```bash
$ simulator <compute_nodes> <trace_file> <max_jobs> <workflow_specification> <start_time> <algorithm> <batch_algorithm> <logging_option> <output_file>

```

### Arguments
```
compute_nodes
	- The number of compute nodes in the cluster
trace_file
	- A workload trace file that defines the "background" load on the cluster throughout. The workflow execution competes with this background load for access to compute nodes
max_jobs
	- The maximum number of jobs allowed in the system per user
workflow_specification
	- A workflow description file that gives workflow task execution times and dependencies	
start_time
	- The date at which the workflow execution begins (i.e., when the first task can be submitted for execution)
algorithm
	- The application-level workflow scheduling algorithm to employ
batch_algorithm
	- The RJMS-level scheduling algorithm used by the batch scheduler
```                              
#### Optional Arguments
```
logging_option
	- Enables some level of logging from WRENCH
output_file
	- Name of json file to write simulation results to
```

Invoking the simulator with no arguments will output a usage menu along with pre-made algorithm options (redacted output):
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
