import subprocess
import time
import random

def print_process_output(res, time):
    for line in res.splitlines():
        print(line.decode("utf-8"))
    print("Simulation took %d seconds" % time)
    print("\n")

def print_command(command):
    print(" ".join(command))

def simulator_command():
    executable = "./build/simulator"
    num_compute_nodes = "128"
    job_trace_file = "../batch_logs/swf_traces_json/kth_sp2.json"
    max_sys_jobs = "1000"
    workflow_specification = "levels:666:50:3600:36000:50:3600:3600:50:3600:36000:50:3600:36000"
    start_time = "100000"
    algorithm = "evan:overlap:pnolimit"
    batch_algorithm = "conservative_bf"
    log = "--wrench-no-log"
    return [executable, num_compute_nodes, job_trace_file, max_sys_jobs, workflow_specification, start_time, algorithm, batch_algorithm, log]

# Increase start time by 25% per run
def vary_start_time(command, num_runs):
    for i in range(0, num_runs):
        start_time = int(command[5])
        new_start_time = int(start_time * 1.25)
        command[5] = str(new_start_time)
        print_command(command)
        run_simulator(command)

def set_algorithm(algorithm, command):
    command[5] = algorithm

# min_level and max_level inclusive
def random_workflow(min_level=2, max_level=5, min_tasks=1, max_tasks=10):
    workflow = ["levels", str(random.randint(1, 1000))]
    num_levels = random.randint(min_level, max_level + 1)
    for _ in range(num_levels):
        num_tasks = str(random.randint(min_tasks, max_tasks))
        start = random.randint(0, 3600)
        end = start + random.randint(0, 36000)
        workflow.extend([num_tasks, str(start), str(end)])
    return ":".join(workflow)

def run_simulator(command):
    try:
        start = time.time()
        # Timeout throws exception, this is okay i guess
        res = subprocess.check_output(command, timeout=6000)
        end = time.time()
        print_process_output(res, end - start)
    except Exception as e:
        print('Exception in simulation: {}\n\n'.format(e)) 

def main():
    # command = "./build/simulator 128 ../batch_logs/swf_traces_json/kth_sp2.json 1000 levels:666:50:3600:36000:50:3600:3600:50:3600:36000:50:3600:36000 100000 zhang:overlap:pnolimit conservative_bf --wrench-no-log"

    # Base test
    command = simulator_command()
    # print_command(command)
    # run_simulator(command)

    # print(random_workflow())
    
    '''
    Runs a random workflow of height 2 - 7
    Each for 10 different time intervals
    Run 3 times to get various workflows
    '''
    for i in range(3):
        for i in range (2, 8):
            command[5] = 100000
            command[4] = random_workflow(min_level=i, max_level=i)
            vary_start_time(command, 10)






if __name__ == "__main__":
    main()
