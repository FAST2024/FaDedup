#!/bin/bash

# parameters
NODE_NUM=3
NODE_NAME=node
USER=wl
HOME=/home/$USER
DPDEDUP_DIR=$HOME/DPDedup

ip_list=(
    192.168.235.201
    192.168.235.202
    192.168.235.203
    192.168.235.204
)

for((i=1;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		host=${NODE_NAME}$i
	fi
    ip=${ip_list[$i]}

    ssh $USER@$host "sed -i \"s/local_ip ${ip_list[0]}/local_ip ${ip}/\" $DPDEDUP_DIR/conf/sys.conf" 
} &
done
wait

