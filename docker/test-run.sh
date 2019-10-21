#!/usr/bin/env bash

sudo docker run tcbs 100 /simulator/trace_files/kth_sp2.json 10 dax:/simulator/workflows/SIPHT_50_360000.dax 86400 static:one_job-100000-1 conservative_bf --wrench-no-log

sudo docker run -v $PWD:/output tcbs 100 /simulator/trace_files/kth_sp2.json 10 dax:/simulator/workflows/SIPHT_50_360000.dax 86400 static:one_job-100000-1 conservative_bf --wrench-no-log /output/test.json