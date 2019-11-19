#!/usr/bin/env bash

echo "Building..."
mkdir -p build/workflows
mkdir -p build/trace_files
# Swap out path to where these files on your system
cp -r data/workflows/* build/workflows/
cp -r data/trace_files/* build/trace_files
# cp config.json build/

# python wrapper for the simulator
cp simulator.py build/
cp run_simulation.sh build/

docker build -t wrenchproject/task-clustering:latest .
rm -rf build
echo "Done building wrenchproject/task-clustering:latest"
