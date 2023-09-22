#!/bin/bash

NODE_NUM=8
NODE_NAME=node
DPDEDUP_DIR=/root/DPDedup

for((i=1;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
    then
        host=${NODE_NAME}0${i}
    else
        host=${NODE_NAME}$i
    fi

    ssh $USER@$host "cd $DPDEDUP_DIR/bin; redis-cli flushall; sudo ./DPAgent"
} &
done
wait
