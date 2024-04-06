#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$SCRIPT_PATH/run_snb.out"
repeat_count="10"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  echo "Q10" >> $out_file
  ${PARENT_PATH}/release/Reservoir "q10" "$data_path/snb.dat" "1000000" >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file
