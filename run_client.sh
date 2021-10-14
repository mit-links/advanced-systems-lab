#!/bin/bash

read args # read from stdin

IFS=' ' read -ra args_arr <<< "$args"

if [[ ${#args_arr[@]} < 9 ]]; then
	echo "not enough args"
	exit 1
fi

clients="${args_arr[0]}"
threads="${args_arr[1]}"
serverip="${args_arr[2]}"
serverport="${args_arr[3]}"
time="${args_arr[4]}"
multiget="${args_arr[5]}"
sets="${args_arr[6]}"
gets="${args_arr[7]}"
inst="${args_arr[8]}"

cmdpart="memtier_benchmark --protocol=memcache_text --key-maximum=10000 --expiry-range=9999-10000 --data-size=1024 --show-config --json-out-file=out0${inst}.json --client-stats=ind_client_out_${inst}"

#add parameters to the command

cmd="${cmdpart} --clients=${clients} --threads=${threads} --server=${serverip} --port=${serverport} --test-time=${time} --multi-key-get=$multiget --ratio=$sets:$gets"
echo "memtier command: ${cmd}"
#run the command
$cmd
	
echo "done running memtier"

