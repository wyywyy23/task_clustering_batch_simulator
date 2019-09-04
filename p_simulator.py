import subprocess
import time
import pymongo
import urllib.parse
from datetime import datetime
from threading import Thread
from threading import Lock

import sys

lock = Lock()
save_to_mongo = False
coll_name = 'default_coll'
commands = []

def simulator_command():
    executable = './build/simulator'
    num_compute_nodes = '100'
    job_trace_file = '../batch_logs/swf_traces_json/kth_sp2.json'
    max_sys_jobs = '100'
    workflow_specification = 'levels:666:50:3600:3600:50:3600:3600:50:3600:3600:50:3600:3600'
    start_time = '100000'
    algorithm = 'evan:overlap:pnolimit'
    batch_algorithm = 'conservative_bf'
    log = '--wrench-no-log'
    return [executable, num_compute_nodes, job_trace_file, max_sys_jobs, workflow_specification, start_time, algorithm, batch_algorithm, log]

# Generates dictionary for mongo storage
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
    dict['wasted_node_seconds'] = -1
    dict['error'] = ''
    # for zhang/evan
    # dict['split'] = False
    return dict

def print_command(command):
    print(" ".join(command))

def print_process_output(command, res, time):
    lock.acquire()
    print_command(command)
    res_arr = []
    for line in res.splitlines():
        line = line.decode("utf-8")
        print(line)
        res_arr.append(line)
    print("Simulation took %d seconds" % time)
    print("\n")
    lock.release()
    return res_arr

def write_to_mongo(obj):
    username = urllib.parse.quote_plus('evan')
    password = urllib.parse.quote_plus('password')
    myclient = pymongo.MongoClient('mongodb://%s:%s@dirt02.ics.hawaii.edu/simulations' % (username, password))
    mydb = myclient["simulations"]
    mycol = mydb[coll_name]
    mycol.insert_one(obj)

def run_simulator(command):
    obj = simulation_dict(command)
    start = time.time()
    end = start
    try:
        # Timeout throws exception, this is okay i guess
        res = subprocess.check_output(command, timeout=600, stderr=subprocess.STDOUT)
        end = time.time()
        res = print_process_output(command, res, end - start)
        obj['success'] = True
        if "test" in command[6] or "evan" in command[6] or "zhang" in command[6]:
            obj['num_splits'] = int((res[len(res) - 7]).split("=")[1])
        else:
            obj['num_splits'] = 0
        obj['makespan'] = float((res[len(res) - 6]).split("=")[1])
        obj['num_p_job_exp'] = float((res[len(res) - 5]).split("=")[1])
        obj['total_queue_wait'] = float((res[len(res) - 4]).split("=")[1])
        obj['used_node_sec'] = float((res[len(res) - 3]).split("=")[1])
        obj['wasted_node_seconds'] = float((res[len(res) - 2]).split("=")[1])
        # for zhang/evan
        # if len(res) > 5:
        #     obj['split'] = True
    except Exception as e:
        lock.acquire()
        print_command(command)
        print('Exception in simulation: {}\n\n'.format(e))
        obj['success'] = False
        obj['error'] = str(e)
        lock.release()

    obj['runtime'] = end - start
    obj['timestamp'] = datetime.utcnow()

    lock.acquire()
    try:
        if save_to_mongo:
            write_to_mongo(obj)
        else:
            pass
    except Exception as e:
        print("Mongo failure")
        print(obj)
        print('Exception in simulation: {}\n\n'.format(e))
    lock.release()

def execute():
    print("started thread")
    has_commands = True
    command = None
    while True:
        lock.acquire()
        if len(commands) == 0:
            command = None
        else:
            print("Running simulation: %d" % (len(commands)))
            command = commands.pop(0)
        lock.release()
        if command:
            run_simulator(command)
            # print_command(command)
        else:
            print("thread exiting")
            return

# TODO - will break if not a levels workflow
def get_algorithm(algorithm, workflow):
    if 'max' not in algorithm:
        return algorithm
    else:
        max_tasks = workflow[11:13]
        return 'static:one_job-' + max_tasks

def get_compute_nodes(trace):
    if 'kth' in trace:
        return '100'
    elif 'sdsc' in trace:
        return '128'
    elif 'gaia' in trace:
        return '2004'
    elif 'ricc' in trace:
        return '8192'
    else:
        print('unknown trace')
        exit()

def create_fork_join(num_levels, tasks_per_level, task_time):
    workflow = 'levels:666'
    task_time = task_time * 3600
    for i in range(0, num_levels):
        f = ':' + str(tasks_per_level) + ':' + str(task_time) + ':' + str(task_time)
        j = ':' + '1' +  ':' + str(task_time) + ':' + str(task_time)
        workflow = workflow + f + j
    return workflow

def main():
    start = time.time()
    trace_files = ['../batch_logs/swf_traces_json/kth_sp2.json']
    # trace_files = ['../batch_logs/swf_traces_json/gaia.json']
    # trace_files = ['../batch_logs/swf_traces_json/kth_sp2.json', '../batch_logs/swf_traces_json/sdsc_sp2.json', '../batch_logs/swf_traces_json/gaia.json', '../batch_logs/swf_traces_json/ricc.json']
    # workflows = ['dax:../m_workflows/m_montage_100.dax', 'dax:../m_workflows/m_epigenomics_100.dax', 'dax:../m_workflows/m_floodplain.dax', 'dax:../m_workflows/m_sipht_100.dax', 'dax:../m_workflows/m_psmerge_small.dax']

    # workflows = [create_fork_join(1, 10, 2), create_fork_join(1, 20, 2), create_fork_join(1, 50, 2), create_fork_join(3, 10, 2), create_fork_join(3, 20, 2), create_fork_join(3, 50, 2), create_fork_join(5, 10, 2), create_fork_join(5, 20, 2), create_fork_join(5, 50, 2), create_fork_join(1, 10, 5), create_fork_join(1, 20, 5), create_fork_join(1, 50, 5), create_fork_join(3, 10, 5), create_fork_join(3, 20, 5), create_fork_join(3, 50, 5), create_fork_join(5, 10, 5), create_fork_join(5, 20, 5), create_fork_join(5, 50, 5), create_fork_join(1, 10, 10), create_fork_join(1, 20, 10), create_fork_join(1, 50, 10), create_fork_join(3, 10, 10), create_fork_join(3, 20, 10), create_fork_join(3, 50, 10), create_fork_join(5, 10, 10), create_fork_join(5, 20, 10), create_fork_join(5, 50, 10)]
    
    # workflows = [create_fork_join(3, 10, 5), create_fork_join(5, 50, 10), create_fork_join(5, 15, 1)]

    workflows = ['dax:../workflows/mont_workflows/montage_50_3600.dax', 'dax:../workflows/mont_workflows/montage_50_18000.dax', 'dax:../workflows/mont_workflows/montage_50_36000.dax', 'dax:../workflows/mont_workflows/montage_100_3600.dax', 'dax:../workflows/mont_workflows/montage_100_18000.dax', 'dax:../workflows/mont_workflows/montage_100_36000.dax', 'dax:../workflows/mont_workflows/montage_250_3600.dax', 'dax:../workflows/mont_workflows/montage_250_18000.dax', 'dax:../workflows/mont_workflows/montage_250_36000.dax', 'dax:../workflows/mont_workflows/montage_500_3600.dax', 'dax:../workflows/mont_workflows/montage_500_18000.dax', 'dax:../workflows/mont_workflows/montage_500_36000.dax']

    # workflows = []
    # for level in [1, 3, 5]:
    #     for length in [2, 5, 10]:
    #         workflows.append(create_fork_join(level, 50, length))
 
    # Take out one_job_max
    # algorithms = ['static:one_job-0-1', 'static:one_job_per_task', 'zhang:overlap:pnolimit', 'test:1:0', 'evan:overlap:pnolimit:1']
    # algorithms = ['test:1:0']
    algorithms = ['zhang_fixed:overlap:pnolimit', 'zhang_fixed_global', 'zhang_fixed_global_prediction', 'static:one_job-0-1', 'static:one_job_per_task', 'test:1:0']
    # num_nodes = [str(x * 10) for x in range(5, 16)]
    num_nodes = ['100']
    start_times = [str(x * 1800) for x in range(48, 385)]
    # start_times = [str(x * 3600) for x in range(100, 200)]
    # workflows = ['levels:666:10:3600:3600:10:3600:3600:10:3600:3600:10:3600:3600:10:3600:3600:10:3600:3600:10:3600:3600:10:3600:3600']

    print('Trace files: ', trace_files)
    print('Number of nodes: ', num_nodes)
    print('Start times: ', start_times)
    print('Workflows: ', workflows)
    print('Algorithms: ', algorithms)
    print('')

    for trace in trace_files:
        for nodes in num_nodes:
            for start_time in start_times:
                for workflow in workflows:
                    for algorithm in algorithms:
                        command = simulator_command()
                        command[1] = nodes
                        command[2] = trace
                        # set max_sys_jobs to number of nodes on machine
                        command[3] = command[1]
                        command[4] = workflow
                        command[5] = start_time
                        command[6] = get_algorithm(algorithm, workflow)
                        commands.append(command)
    '''
    commands.clear()
    fd = open('unfinished_b5.txt', 'r')
    line = fd.readline()
    while line:
        com = line[2:-3].split("\', \'")
        commands.append(com)
        line = fd.readline()
    '''
    print("Total simulations to run: %d" % len(commands))
    
    threads = []
    cores = 10

    for i in range(cores):
        thread = Thread(target=execute)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    print("\n\nSimulations took %d seconds" % (time.time() - start))

# Only save to mongo if a collection name is provided
if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Not saving to mongo")
    elif len(sys.argv) == 2:
        print("Saving results to %s" % sys.argv[1])
        save_to_mongo = True
        coll_name = sys.argv[1]
    else:
        print("invalid number of arguments")
        exit()
    main()
