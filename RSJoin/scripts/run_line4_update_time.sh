#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$SCRIPT_PATH/run_line4_update_time.out"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

${PARENT_PATH}/release/Reservoir "line_join_update_time" "4" "$data_path/epinions_4.txt" >> $out_file 2>&1
