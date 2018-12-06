import subprocess
import time
import random
import pymongo
import urllib.parse

# TODO - swap out hardcoded valued for command[]
# TODO - maybe don't vary the random seed
def simulation_dict(command):
    dict = {}
    dict['executable'] = command[0]
    dict['num_compute_nodes'] = command[1]
    dict['job_trace_file'] = command[2]
    dict['max_sys_jobs'] = command[3]
    dict['workflow_specification'] = command[4]
    dict['start_time'] = command[5]
    dict['algorithm'] = command[6]
    dict['batch_algorithm'] = command[7]
    dict['log'] = command[8]
    # Store run results!
    dict['success'] = False
    dict['runtime'] = -1
    dict['makespan'] = -1
    dict['num_p_job_exp'] = -1
    dict['total_queue_wait'] = -1
    dict['used_node_sec'] = -1
    dict['wasted_node_time'] = -1
    dict['error'] = ''
    # for zhang/evan
    dict['split'] = False
    return dict



def print_process_output(res, time):
    res_arr = []
    for line in res.splitlines():
        line = line.decode("utf-8")
        print(line)
        res_arr.append(line)
    print("Simulation took %d seconds" % time)
    print("\n")
    return res_arr

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
    command[6] = algorithm

# min_level and max_level inclusive
# returns max # of tasks in all levels
def random_workflow(min_level=2, max_level=5, min_tasks=1, max_tasks=10):
    workflow = ["levels", "666"]
    num_levels = random.randint(min_level, max_level)
    most_tasks = -1
    for _ in range(num_levels):
        num_tasks = str(random.randint(min_tasks, max_tasks))
        start = random.randint(0, 3600)
        end = start + random.randint(0, 36000)
        workflow.extend([num_tasks, str(start), str(end)])
        most_tasks = max(most_tasks, int(num_tasks))
    return ":".join(workflow), most_tasks

def deterministic_workflow(num_levels):
    workflow = ["levels", "666"]
    for _ in range(num_levels):
        workflow.extend(["50", "18000", "18000"])
    return ":".join(workflow), 50

def run_simulator(command):
    obj = simulation_dict(command)
    start = time.time()
    end = start
    try:
        # Timeout throws exception, this is okay i guess
        res = subprocess.check_output(command, timeout=18000)
        end = time.time()
        res = print_process_output(res, end - start)
        obj['success'] = True
        obj['makespan'] = float((res[len(res) - 6]).split("=")[1])
        obj['num_p_job_exp'] = float((res[len(res) - 5]).split("=")[1])
        obj['total_queue_wait'] = float((res[len(res) - 4]).split("=")[1])
        obj['used_node_sec'] = float((res[len(res) - 3]).split("=")[1])
        obj['wasted_node_time'] = float((res[len(res) - 2]).split("=")[1])
        # for zhang/evan
        if len(res) > 5:
            obj['split'] = True
    except Exception as e:
        print('Exception in simulation: {}\n\n'.format(e))
        obj['success'] = False
        obj['error'] = e

    obj['runtime'] = end - start

    try:
        username = urllib.parse.quote_plus('evan')
        password = urllib.parse.quote_plus('password')
        myclient = pymongo.MongoClient('mongodb://%s:%s@dirt02.ics.hawaii.edu/simulations' % (username, password))
        mydb = myclient["simulations"]
        mycol = mydb["test_col"]
        mycol.insert_one(obj)
    except Exception as e:
        print("Mongo failure")
        print(obj)
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
    Runs each algorithm per workflow/time
    '''
    # for i in range(3):
    levels = [2, 5, 10, 15, 20]
    for i in range (0, len(levels)):
        # command[4], max_tasks = random_workflow(min_level=i, max_level=i, min_tasks=1, max_tasks=50)
        command[4], max_tasks =  deterministic_workflow(levels[i])
        # username = urllib.parse.quote_plus('evan')
        # password = urllib.parse.quote_plus('password')
        # myclient = pymongo.MongoClient('mongodb://%s:%s@dirt02.ics.hawaii.edu/simulations' % (username, password))
        # mydb = myclient["simulations"]
        # mycol = mydb["workflows3"]
        # mycol.insert_one({"workflow":command[4]})

        set_algorithm("test:overlap:pnolimit", command)
        command[5] = 100000
        vary_start_time(command, 10)

        '''
        print("\nWORKFLOW: " + command[4] + "\n\n")
        print("\nRUNNING: evan:overlap:pnolimit\n\n")
        # Run evan
        set_algorithm("evan:overlap:pnolimit", command)
        command[5] = 100000
        vary_start_time(command, 10)
        print("\nRUNNING: zhang:overlap:pnolimit\n\n")
        # Run zhang
        set_algorithm("zhang:overlap:pnolimit", command)
        command[5] = 100000
        vary_start_time(command, 10)
        print("\nRUNNING: static:one_job_per_task\n\n")
        # Run static:one_job_per_task
        set_algorithm("static:one_job_per_task", command)
        command[5] = 100000
        vary_start_time(command, 10)
        print("\nRUNNING: static:one_job-0\n\n")
        # Run one_job but pick best # of nodes
        set_algorithm("static:one_job-0", command)
        command[5] = 100000
        vary_start_time(command, 10)
        print("\nRUNNING: static:one_job-max\n\n")
        # Run one_job but pick #nodes=largest # of tasks in any level
        set_algorithm(("static:one_job-" + str(max_tasks)), command)
        command[5] = 100000
        vary_start_time(command, 10)
        '''





if __name__ == "__main__":
    main()

