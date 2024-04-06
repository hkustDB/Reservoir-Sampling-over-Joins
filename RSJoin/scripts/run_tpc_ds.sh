#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$SCRIPT_PATH/run_tpc_ds.out"
repeat_count="10"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/release/Reservoir "qx" "$data_path/qx_sf10.dat" "1000000" >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/release/Reservoir "qy" "$data_path/qy_sf10.dat" "1000000" >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file

current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/release/Reservoir "qz" "$data_path/qz_sf10.dat" "1000000" >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file