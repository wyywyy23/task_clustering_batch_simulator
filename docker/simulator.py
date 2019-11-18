
from sys import argv
import subprocess
import json

# If unable to parse output file argument
# OUTPUT_FILE_PATH = '/output/error.json'
OUTPUT_FILE_PATH = 'error.json'

OUTPUT_JSON = {
    "algorithm": "zhang:noglobal:nobsearch:prediction",
    "batch_algorithm": "conservative_bf",
    "end_levels": [
        0,
        3
    ],
    "individual_mode": True,
    "job_trace_file": "/simulator/trace_files/kth_sp2.json",
    "makespan": 183236.48000004218,
    "max_sys_jobs": "100",
    "num_compute_nodes": "100",
    "num_p_job_exp": 0,
    "num_splits": 1,
    "start_time": "104400",
    "total_queue_wait": 0.0,
    "used_node_sec": 3607179.9800000004,
    "wasted_node_seconds": 1948.120000016901,
    "workflow_specification": "dax:/simulator/workflows/CYBERSHAKE_250_3600000.dax"
}


def write_dict_to_file(data, file_name):
    with open(file_name, 'w') as outfile:
        json.dump(data, outfile)
        outfile.close()


def main(num_compute_nodes, job_trace_file, max_sys_jobs, workflow_specification, start_time, algorithm, batch_algorithm, wrench_log, output_file):
    cmd = ["./simulator", num_compute_nodes, job_trace_file, max_sys_jobs, workflow_specification, start_time, algorithm, batch_algorithm, wrench_log, output_file]
    try:
        # Timeout throws an exception
        res = subprocess.check_output(cmd, timeout=3600, stderr=subprocess.STDOUT)
        # res captures all stdout and stderr, but we don't need it
        # cpp simulator writes out json by itself if success
    except Exception as e:
        output_json = {
            "error": str(e),
            "algorithm": algorithm,
            "batch_algorithm": batch_algorithm,
            "job_trace_file": job_trace_file,
            "max_sys_jobs": str(max_sys_jobs),
            "num_compute_nodes": str(num_compute_nodes),
            "start_time": str(start_time),
            "workflow_specification": workflow_specification,
            # will not be used, but include anyways
            "wrench_log": wrench_log,
            "output_file": output_file
        }
        write_dict_to_file(output_json, output_file)
    return 0


# Example: 100 /simulator/trace_files/kth_sp2.json 10 dax:/simulator/workflows/SIPHT_50_360000.dax 86400 zhang:noglobal:bsearch:prediction conservative_bf --wrench-no-log /output/sipht.json

if __name__ == '__main__':
    if len(argv) != 10:
        print("Simulator requires 9 arguments:  <num_compute_nodes> <job trace file> <max jobs in system> <workflow specification> <workflow start time> <algorithm> <batch algorithm> --wrench-no-log <json file to write results to>")
        write_dict_to_file({"error": "Invalid number of args", "args": argv}, OUTPUT_FILE_PATH)
    main(*argv[1:])
