#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$SCRIPT_PATH/run_qz_scale_factor.out"
sfs="1 3 10 30"
repeat_count="10"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

for sf in ${sfs[@]}; do
  echo "SF = $sf" >> $out_file
	current_repeat=1
	while [[ ${current_repeat} -le ${repeat_count} ]]
  do
    ${PARENT_PATH}/release/Reservoir "qz" "$data_path/qz_sf$sf.dat" "1000000" >> $out_file 2>&1
    current_repeat=$((current_repeat+1))
  done
	echo "" >> $out_file
done