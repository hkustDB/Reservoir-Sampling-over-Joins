#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$SCRIPT_PATH/run_tpc_ds_qy.out"
repeat_count="10"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  echo "Qy" >> $out_file
  ${PARENT_PATH}/release/driver -a "sjoin" -q "tpcds_qy" -i "$data_path/qy_sf10.dat" -s swor -r "1000000" -n >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
  echo "" >> $out_file
done
