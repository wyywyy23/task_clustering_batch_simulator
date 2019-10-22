# Docker Usage

The built docker image is hosted on and and can be pulled from XXX.

The dockerfile is built on top of wrench's batsched image, then installs simgrid, wrench, and the batch simulator repos.

The simulator is invoked with:
```bash
./simulator <num_compute_nodes> <job trace file> <max jobs in system> <workflow specification> <workflow start time> <algorithm> <batch algorithm> --wrench-no-log <json file to write results to>
```
Providing the "\<json file to write results to\>" argument will write the results of the simulation in json format to the provided file name. However, if the simulation fails or throws an error, the file will NOT be created. There is currently no way to catch all runtime errors, so some simulation results will be lost unless stdout and sterr output is saved.

## config.json
All of the arguments required should be generated from the provided config.json. All necessary trace and workflow files are built into the image.

The 'trace_files' field contains a list of the trace files for machines we simulated. For each file in trace_files, file[0] is the name of the file, and file[1] is the number of compute nodes on the machine. There is also a "trace_file_dir" field that must be pre-appended to each file name, so the file path can be found within the container e.g. kth_sp2.json is stored at "/simulator/trace_files/kth_sp2.json" in the container.

Similarly for workflow files, the "workflow_dir" and "workflow_type" must both be pre-pended to every workflow file listed in "workflows" e.g. "dax:/simulator/workflows/SIPHT_50_360000.dax". The "dax:" part tells the simulator that we are running a dax file.

## Run the container
For testing, I mounted /output on the container to my host's working directory, so I specified the json file to be written to /output in the command.
```bash
docker run -v $PWD:/output wrenchproject/task-clustering:latest <trace_file[1]> <trace_file_dir + trace_file[0]> <max_sys_jobs> <"dax:" + workflow_dir + wokflow_file> <start_time> <algorithm> conservative_bf --wrench-no-log <"/output/" + file_name.json>
```

We only use conservative backfilling, so the batch algorithm can always be hardcoded as "conservative_bf". We also only run with "--wrench-no-log", so that should be hardcoded as well.

- Run set #1: Zhang + one job per task with varying maximum number of jobs allowed in the system.
    - Use config.json

- Run set #2: ....
    - More configs will be provided for future tests...

