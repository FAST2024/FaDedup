#!/bin/bash

# parameters
NODE_NUM=8
NODE_NAME=node
HOME=/root/$USER
DPDEDUP_DIR=$HOME/DPDedup

ip_list=(
    192.168.235.195
    192.168.235.170
    192.168.235.172
    192.168.235.171
)

for((i=1;i<=$NODE_NUM;i++));
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		host=${NODE_NAME}$i
	fi

    ssh $USER@$host "sudo killall DPAgent; redis-cli flushall"
} &
done
wait
