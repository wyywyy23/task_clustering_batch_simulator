#!/usr/bin/env bash

echo "Building..."
mkdir -p build/workflows
mkdir -p build/trace_files
# Swap out path to where these files on your system
cp -r ../../workflow-generator/workflows/* build/workflows/
cp -r ../../batch_logs/swf_traces_json/* build/trace_files
# cp config.json build/
docker build -t tcbs .
rm -rf build
echo "Done building."