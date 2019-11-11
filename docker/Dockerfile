FROM wrenchproject/wrench-build:ubuntu-bionic-gcc7-batsched AS simgrid

MAINTAINER Evan Hataishi <evanhata@hawaii.edu>

USER root

RUN wget https://framagit.org/simgrid/simgrid/uploads/ddd14d9e34ee36bc90d9107f12480c28/SimGrid-3.24.tar.gz && tar -xf SimGrid-3.24.tar.gz && cd SimGrid-3.24 && cmake . && make -j8 && make install && cd .. && rm -rf SimGrid-3.24*

FROM simgrid AS wrench

WORKDIR /wrench

RUN git clone --single-branch --branch evan-tmp https://github.com/wrench-project/wrench.git && cd wrench && cmake -DENABLE_BATSCHED=on . && make -j8 && make install

FROM wrench

WORKDIR /simulator

RUN git clone --single-branch --branch evan-dev https://github.com/wrench-project/task_clustering_batch_simulator.git  && cd task_clustering_batch_simulator && cmake . && make -j8

# These files first copied locally as part of the setup from docker-build.sh

COPY build/workflows/* /simulator/workflows/

COPY build/trace_files/* /simulator/trace_files/

WORKDIR /simulator/task_clustering_batch_simulator

RUN sysctl -w vm.max_map_count=131072

#ENTRYPOINT ["./simulator"]

CMD []
