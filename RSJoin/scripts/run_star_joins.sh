#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$SCRIPT_PATH/run_star_joins.out"
repeat_count="10"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/release/Reservoir "star_join" "4" "$data_path/epinions_4.txt" "100000" >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/release/Reservoir "star_join" "5" "$data_path/epinions_5.txt" "100000" >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/release/Reservoir "star_join" "6" "$data_path/epinions_6.txt" "100000" >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file