#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

out_file="$PARENT_PATH/run_predicate_input_size.out"
repeat_count="10"
data_path="/path/to/data"
rm -f $out_file
touch $out_file

echo "RS with Predicate" >> $out_file
current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/Reservoir "predicate" "$data_path/predicate_1_progress.txt" "1000" "nPwNqQGWBUaoZznaPzTfVIyspLRUUSZirmmaGiknBsdNrKDQbnJxXPtpRNIjJhNOwbpZKbZczWDEAtiGKJPaUAVnlQpfeiAgilDdsAsYCCbivZEiHjJKwCRjdSHPTGUCoDTOjVaRbMHOwaDfIYouvWRtxNNwZEtTrvEZYJjpepeUrXEQfUONSkjZQIVBPlEmDlkaWZCKDXcVxdpfRfYzYTUfbnmSuxphsjKLhkLuNEdUFYqexGONRMfiurIgkormNmNKpgXSmvEKYriWVjImmnBENmXXbByKwbTvnvGOIAzeYfWkrQrnNvtELifdHBDtrFvRaxrSQrVIngCaYQdLxFsRGqYsSHyXqstciAimtoqJIBxlfXTPDQxQwGWcakqkDgFZAtNShYCkcrztuZaNVimqlSVYCoEYsVrbKTpOOKlyZKVRPSUYRgntXylIeCHDbMPeyYgFZhriJRqLDLzJiVdOUyKZnPIxSPlvkJapGTQEglPSarBnRPctmzkOkTuhRMJiiGiIoXBpYxMhMZonAXjKXUnDNKmBqeLZGuRIVykwXaousrCbaIuMfYjpfHTXfpTrrmPHbGKqMkEPfhZSbUGpoWYAoOaAtFVxiJOGbJbeMXvdVlTZkQnkUnnGLFKoAaBNWcddQCPoNOtUmcCbcvwoIgulFCgwstDrTPSMIJXFJezAOTJVLzObMGqNbNQmMlLFVGRbhmpUMKVhURzSZpUbkvrFKUNqlhtdxIPaotCmkkuLNVKOvEfBzdKhylfODHeDdxEgFOHyrZfaRNpyUByFuvFOtsLVyuAiwnHvkiiowkQWMNfZscWvpNOLtPxfafoCHIotszEJvHvytnmYkZWvMbHJNgEvGsGMsthcWnWhFvXcZdxGbzHRHmWPZHxXPTPHqtveKZqOZvGukZEftqVdxwZhmlXRRHNMZZAoYyqGrsEtaBvkCEvqLHyUrKeasXaqidgSLoVVcmmeDEGAbbRaNOvncGkqPyEiHQCAtAmWzFUzWGdczwIDPRiTBkyt" "16" >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file

echo "RS without Predicate" >> $out_file
current_repeat=1
while [[ ${current_repeat} -le ${repeat_count} ]]
do
  ${PARENT_PATH}/Reservoir "predicate_baseline" "$data_path/predicate_1_progress.txt" "1000" "nPwNqQGWBUaoZznaPzTfVIyspLRUUSZirmmaGiknBsdNrKDQbnJxXPtpRNIjJhNOwbpZKbZczWDEAtiGKJPaUAVnlQpfeiAgilDdsAsYCCbivZEiHjJKwCRjdSHPTGUCoDTOjVaRbMHOwaDfIYouvWRtxNNwZEtTrvEZYJjpepeUrXEQfUONSkjZQIVBPlEmDlkaWZCKDXcVxdpfRfYzYTUfbnmSuxphsjKLhkLuNEdUFYqexGONRMfiurIgkormNmNKpgXSmvEKYriWVjImmnBENmXXbByKwbTvnvGOIAzeYfWkrQrnNvtELifdHBDtrFvRaxrSQrVIngCaYQdLxFsRGqYsSHyXqstciAimtoqJIBxlfXTPDQxQwGWcakqkDgFZAtNShYCkcrztuZaNVimqlSVYCoEYsVrbKTpOOKlyZKVRPSUYRgntXylIeCHDbMPeyYgFZhriJRqLDLzJiVdOUyKZnPIxSPlvkJapGTQEglPSarBnRPctmzkOkTuhRMJiiGiIoXBpYxMhMZonAXjKXUnDNKmBqeLZGuRIVykwXaousrCbaIuMfYjpfHTXfpTrrmPHbGKqMkEPfhZSbUGpoWYAoOaAtFVxiJOGbJbeMXvdVlTZkQnkUnnGLFKoAaBNWcddQCPoNOtUmcCbcvwoIgulFCgwstDrTPSMIJXFJezAOTJVLzObMGqNbNQmMlLFVGRbhmpUMKVhURzSZpUbkvrFKUNqlhtdxIPaotCmkkuLNVKOvEfBzdKhylfODHeDdxEgFOHyrZfaRNpyUByFuvFOtsLVyuAiwnHvkiiowkQWMNfZscWvpNOLtPxfafoCHIotszEJvHvytnmYkZWvMbHJNgEvGsGMsthcWnWhFvXcZdxGbzHRHmWPZHxXPTPHqtveKZqOZvGukZEftqVdxwZhmlXRRHNMZZAoYyqGrsEtaBvkCEvqLHyUrKeasXaqidgSLoVVcmmeDEGAbbRaNOvncGkqPyEiHQCAtAmWzFUzWGdczwIDPRiTBkyt" "16" >> $out_file 2>&1
  current_repeat=$((current_repeat+1))
done
echo "" >> $out_file