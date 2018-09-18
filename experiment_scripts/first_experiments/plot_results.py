#!/usr/bin/env python2.7

import sys
import numpy
import matplotlib.pyplot as pyplot
import math

if __name__ == '__main__':

    if (len(sys.argv) != 2):
        print >> sys.stderr, "Usage: ./plot_results.py <result file>"
        exit(-1)

    fname = sys.argv[1]

    # Read all input
    with open(fname) as f:
        lines = f.readlines()
    lines = [x.strip() for x in lines] 

    pyplot.figure()
    #pyplot.title('Activity #1, Step #1')
    pyplot.xlabel('Submit time')
    pyplot.ylabel('Makespan')

    num_results = 0
    markers = []
    plotlines = []
    labels = []

    # Find width
    min_start_time = -1;
    for line in lines:
        if line[0] != 'a':
            continue
        tokens = line.split('|')
        start_time = float(tokens[1].split("=")[1])
        if min_start_time == -1:
            min_start_time = start_time
        time_span = start_time - min_start_time


    for line in lines:
        #print "line=", line
        #print "line[0]=", line[0]
        if line[0] != 'a':
            continue
        tokens = line.split('|')
        #print "tokens=", tokens
        algorithm = tokens[0].split("=")[1]
        start_time = float(tokens[1].split("=")[1])
        makespan = float(tokens[2].split("=")[1])
        if makespan == -1:
            continue
        num_results += 1

        print algorithm, start_time, makespan
        offset_base = time_span / 250
        if algorithm == "static:one_job-0":
            marker = "r+"
            offset = 0 * offset_base
            plot = True
        elif ((algorithm == "static:one_job-1") or (algorithm == "static:one_job-2") or (algorithm == "static:one_job-3") or (algorithm == "static:one_job-4") or (algorithm == "static:one_job-5") or (algorithm == "static:one_job-6") or (algorithm == "static:one_job-7") or (algorithm == "static:one_job-8") or (algorithm == "static:one_job-9") or (algorithm == "static:one_job-10") or (algorithm == "static:one_job-11") or (algorithm == "static:one_job-12") or (algorithm == "static:one_job-13") or (algorithm == "static:one_job-14") or (algorithm == "static:one_job-15") or (algorithm == "static:one_job-16") or (algorithm == "static:one_job-17") or (algorithm == "static:one_job-18") or (algorithm == "static:one_job-19") or (algorithm == "static:one_job-20") or (algorithm == "static:one_job-21") or (algorithm == "static:one_job-22") or (algorithm == "static:one_job-23") or (algorithm == "static:one_job-24") or (algorithm == "static:one_job-25") or (algorithm == "static:one_job-26") or (algorithm == "static:one_job-27") or (algorithm == "static:one_job-28") or (algorithm == "static:one_job-29") or (algorithm == "static:one_job-30")):
            marker = "r+"
            offset = 0 * offset_base
            plot = True
        elif algorithm == "levelbylevel:overlap:one_job-0":
            marker = "g+"
            offset = 2 * offset_base
            plot = True
        elif algorithm == "zhang:overlap:pnolimit":
            marker = "cx"
            offset = 3 * offset_base
            plot = True
        elif algorithm == "zhang:nooverlap:pnolimit":
            marker = "c+"
            offset = 4 * offset_base
            plot = True
        else:
            marker = "k+"
            offset = 5 * offset_base
            plot = True

        if (plot):
            plotline, = pyplot.plot([start_time + offset], [makespan], marker)
            if not marker in markers:
                    markers.append(marker)
                    plotlines.append(plotline)
                    labels.append(algorithm)
     

    pyplot.legend(plotlines, labels, loc='upper center', bbox_to_anchor=(0.5, +1.10), prop={'size': 8})

    pyplot.savefig('tmp.pdf')
    print "Figure saved in tmp.pdf (" + str(num_results)+ " data points)"


