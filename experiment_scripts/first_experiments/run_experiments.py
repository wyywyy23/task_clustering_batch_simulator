#!/usr/bin/env python2.7

import os
import sys
import subprocess
import numpy
import matplotlib.pyplot as pyplot
import math


# Function to run the simulator and returns the float output values
#
def runSimulator(command_line):
        # Run command-line and get output line
        tokenized_command_line = command_line.split();
	try:
        	output = subprocess.check_output(tokenized_command_line)
	except subprocess.CalledProcessError:
		print >> sys.stderr, "This command has failed:"
		print >> sys.stderr, "    ", command_line
                print >> sys.stderr, "Trying again..."
                try:
        	    output = subprocess.check_output(tokenized_command_line)
                except subprocess.CalledProcessError:
                    print >> sys.stderr, "Failed AGAIN!"
                    return [-1, -1, -1, -1, -1]

        lines = output.split('\n')


        # Look for and process the "MAKESPAN:..." line
        makespan = -1
        num_pilot_job_expirations = -1
        total_queue_wait_seconds = -1
        used_node_seconds = -1
        wasted_node_seconds = -1
        for l in lines:
            tokens = l.split("=")
            if (tokens[0] == "MAKESPAN"):
                if makespan != -1:
                    print >> sys.stderr, "Command output has two 'MAKESPAN:...' lines! Declaring it a failure."
                    makespan = -1
                try:
                    makespan = float(tokens[1])
                except ValueError as e: 
                    print >> sys.stderr, "Non-float makespan value ...", tokens[1],"! Declaring it a failre."
                    makespan = -1.0

            if (tokens[0] == "NUM PILOT JOB EXPIRATIONS"):
                try:
                    num_pilot_job_expirations = float(tokens[1])
                except ValueError as e: 
                    num_pilot_job_expirations - 1

            if (tokens[0] == "TOTAL QUEUE WAIT SECONDS"):
                try:
                    total_queue_wait_seconds = float(tokens[1])
                except ValueError as e: 
                    total_queue_wait_seconds - 1

            if (tokens[0] == "USED NODE SECONDS"):
                try:
                    used_node_seconds = float(tokens[1])
                except ValueError as e: 
                    used_node_seconds - 1

            if (tokens[0] == "WASTED NODE SECONDS"):
                try:
                    wasted_node_seconds = float(tokens[1])
                except ValueError as e: 
                    wasted_node_seconds - 1

        return [makespan, num_pilot_job_expirations, total_queue_wait_seconds, used_node_seconds, wasted_node_seconds]



if __name__ == '__main__':

    if (len(sys.argv) != 2):
        print >> sys.stderr, "Usage: ./run_experiments.py <output file>"
        exit(-1)

    output_file_name = sys.argv[1]


    num_compute_nodes = 100
    max_num_jobs = 1000
    task_duration_min = 600
    task_duration_max = 600
    num_levels = 5;
    num_tasks_per_level = 50
    workflow_config="levels:666"
    for l in xrange(0, num_levels, 2):
        workflow_config += ":"+str(num_tasks_per_level)+":"+str(task_duration_min)+":"+str(task_duration_max)
    workload_trace_file = "../../../batch_logs/swf_traces_json/kth_sp2.json"
    #workload_trace_file = "../../../batch_logs/swf_traces_json/gaia.json"
    #workload_trace_file = "../../../batch_logs/swf_traces_json/metacentrum.json"
    #workload_trace_file = "../../../batch_logs/swf_traces_json/ricc.json"
    #workload_trace_file = "../../../batch_logs/swf_traces_json/sdsc_sp2.json"


    f = open(output_file_name,"w+")
    print >> f, "Workload trace file:", workload_trace_file
    print >> f, "Workflow config:",workflow_config
    f.close()

    algorithm_list = [];

    #for i in xrange(1, num_tasks_per_level+1):
    #    algorithm_list.append("static:one_job-"+str(i))

    algorithm_list.append("static:one_job-10")
    algorithm_list.append("static:one_job-0")
    #algorithm_list.append("levelbylevel:overlap:hc-5-0")
    #algorithm_list.append("levelbylevel:overlap:hc-10-0")
    #algorithm_list.append("levelbylevel:overlap:hc-50-0")
    #algorithm_list.append("static:one_job_per_task")
    #algorithm_list.append("zhang:overlap:pnolimit")
    #algorithm_list.append("zhang:nooverlap:pnolimit")



    for workflow_start_time in xrange(1*24*3600, 2*24*3600, 1*600):
        for algorithm in algorithm_list:
        	command_line = "../../simulator " + str(num_compute_nodes) + " " + workload_trace_file + " " + str(max_num_jobs) + " " + workflow_config + " " +  str(workflow_start_time) + " " + algorithm + " conservative_bf --wrench-no-log"
                f = open(output_file_name,"a")
        	print >> f, command_line
        	print >> sys.stderr, command_line
                f.close()
        	[makespan, num_pilot_job_expirations, total_queue_wait_seconds, used_node_seconds, wasted_node_seconds] = runSimulator(command_line)
                f = open(output_file_name,"a")
                print >> f, "algorithm="+algorithm+"|start_time="+str(workflow_start_time)+"|makespan="+str(makespan)+"|num_pilot_job_expirations="+str(num_pilot_job_expirations)+"|total_queue_wait_seconds="+str(total_queue_wait_seconds)+"|used_node_seconds="+str(used_node_seconds)+"|wasted_node_seconds="+str(wasted_node_seconds)
                f.close()
                print >> sys.stderr, "algorithm="+algorithm+"|start_time="+str(workflow_start_time)+"|makespan="+str(makespan)+"|num_pilot_job_expirations="+str(num_pilot_job_expirations)+"|total_queue_wait_seconds="+str(total_queue_wait_seconds)+"|used_node_seconds="+str(used_node_seconds)+"|wasted_node_seconds="+str(wasted_node_seconds)
