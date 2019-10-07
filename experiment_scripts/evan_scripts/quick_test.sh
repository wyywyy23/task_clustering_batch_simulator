#!/bin/bash

MAIN_DIR=../..

$MAIN_DIR/build/simulator 100 $MAIN_DIR/NASA-iPSC-1993-3.swf 100 levels:42:50:3600:3600:50:3600:3600:50:3600:3600:50:3600:3600 0 static:one_job-0-1 conservative_bf --wrench-no-log

echo

$MAIN_DIR/build/simulator 100 $MAIN_DIR/NASA-iPSC-1993-3.swf 100 levels:42:50:3600:3600:50:3600:3600:50:3600:3600:50:3600:3600 0 static:one_job_per_task conservative_bf --wrench-no-log

echo

$MAIN_DIR/build/simulator 100 $MAIN_DIR/NASA-iPSC-1993-3.swf 100 levels:42:50:3600:3600:50:3600:3600:50:3600:3600:50:3600:3600 0 zhang:overlap:pnolimit conservative_bf --wrench-no-log

echo

$MAIN_DIR/build/simulator 100 $MAIN_DIR/NASA-iPSC-1993-3.swf 100 levels:42:50:3600:3600:50:3600:3600:50:3600:3600:50:3600:3600 0 zhang_fixed:overlap:pnolimit conservative_bf --wrench-no-log

echo

$MAIN_DIR/build/simulator 100 $MAIN_DIR/NASA-iPSC-1993-3.swf 100 levels:42:50:3600:3600:50:3600:3600:50:3600:3600:50:3600:3600 0 evan:overlap:pnolimit:1 conservative_bf --wrench-no-log

echo

$MAIN_DIR/build/simulator 100 $MAIN_DIR/NASA-iPSC-1993-3.swf 100 levels:42:50:3600:3600:50:3600:3600:50:3600:3600:50:3600:3600 0 test:1:1 conservative_bf --wrench-no-log