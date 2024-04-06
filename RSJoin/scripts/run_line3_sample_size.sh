#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$SCRIPT_PATH/run_line3_sample_size.out"
ks="10000 20000 50000 100000 200000 500000 1000000 2000000 5000000"
repeat_count="10"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

for k in ${ks[@]}; do
	echo "k = $k" >> $out_file
	current_repeat=1
	while [[ ${current_repeat} -le ${repeat_count} ]]
  do
    ${PARENT_PATH}/release/Reservoir "line_join" "3" "$data_path/epinions_3.txt" "$k" >> $out_file 2>&1
    current_repeat=$((current_repeat+1))
  done
	echo "" >> $out_file
done
