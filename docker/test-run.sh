#!/usr/bin/env bash

sudo docker run -v $PWD:/output wrenchproject/task-clustering:latest 100 /simulator/trace_files/kth_sp2.json 10 dax:/simulator/workflows/SIPHT_50_360000.dax 86400 zhang:noglobal:bsearch:prediction conservative_bf --wrench-no-log /output/sipht.json

sudo docker run -v $PWD:/output wrenchproject/task-clustering:latest 100 /simulator/trace_files/kth_sp2.json 10 dax:/simulator/workflows/MONTAGE_50_360000.dax 86400 zhang:noglobal:bsearch:prediction conservative_bf --wrench-no-log /output/montage.json

sudo docker run -v $PWD:/output wrenchproject/task-clustering:latest 100 /simulator/trace_files/kth_sp2.json 10 dax:/simulator/workflows/CYBERSHAKE_50_360000.dax 86400 zhang:noglobal:bsearch:prediction conservative_bf --wrench-no-log /output/cshake.json

sudo docker run -v $PWD:/output wrenchproject/task-clustering:latest 100 /simulator/trace_files/kth_sp2.json 10 dax:/simulator/workflows/GENOME_50_360000.dax 86400 zhang:noglobal:bsearch:prediction conservative_bf --wrench-no-log /output/genome.json

sudo docker run -v $PWD:/output wrenchproject/task-clustering:latest 100 /simulator/trace_files/kth_sp2.json 100 dax:/simulator/workflows/CYBERSHAKE_250_3600000.dax 104400 zhang:noglobal:nobsearch:prediction conservative_bf --wrench-no-log /output/test.json

sudo docker run -v $PWD:/output wrenchproject/task-clustering:latest python3 -u simulator.py 100 /simulator/trace_files/kth_sp2.json 100 dax:/simulator/workflows/CYBERSHAKE_250_3600000.dax 104400 zhang:noglobal:nobsearch:prediction conservative_bf --wrench-no-log /output/test.json
