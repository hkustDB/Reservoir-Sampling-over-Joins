#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$SCRIPT_PATH/run_line4_update_time.out"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

${PARENT_PATH}/release/driver -a "sjoin_opt" -q "tpcds_ql4" -i "$data_path/epinions_4.txt" -s swor -r "0" -n >> $out_file 2>&1
