#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$SCRIPT_PATH/run_line3_input_size.out"
repeat_count="10"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/release/driver -a "sjoin_opt" -q "tpcds_ql3" -i "$data_path/epinions_3_progress.txt" -s swor -r "10000" -n >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
  echo "" >> $out_file
done
echo "" >> $out_file
