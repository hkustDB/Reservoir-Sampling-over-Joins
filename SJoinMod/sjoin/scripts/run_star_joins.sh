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
  ${PARENT_PATH}/release/driver -a "sjoin" -q "tpcds_qstar4" -i "$data_path/epinions_4.txt" -s swor -r "100000" -n >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/release/driver -a "sjoin" -q "tpcds_qstar5" -i "$data_path/epinions_5.txt" -s swor -r "100000" -n >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/release/driver -a "sjoin" -q "tpcds_qstar6" -i "$data_path/epinions_6.txt" -s swor -r "100000" -n >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file