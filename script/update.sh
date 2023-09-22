#!/bin/bash

NODE_NUM=8
NODE_NAME=node
HOME=/root
DPDEDUP_DIR=$HOME/DPDedup
MODE=$1

ip_list=(
    192.168.0.1 192.168.0.2 192.168.0.3 192.168.0.4 192.168.0.5 192.168.0.6 192.168.0.7 192.168.0.8 192.168.0.9 192.168.0.10 192.168.0.11 192.168.0.12 192.168.0.13 192.168.0.14 192.168.0.15 192.168.0.16 192.168.0.17 192.168.0.18 192.168.0.19 192.168.0.20 192.168.0.21 192.168.0.22 192.168.0.23 192.168.0.24 192.168.0.25 192.168.0.26 192.168.0.27 192.168.0.28 192.168.0.29 192.168.0.30 192.168.0.31 192.168.0.32
)

if [[ $MODE = "program" ]]
then
    for((i=1;i<=$NODE_NUM;i++))
    do
    {
        if [[ $i -gt 0 && $i -lt 10 ]]
        then
            host=${NODE_NAME}0${i}
        else
            host=${NODE_NAME}$i
        fi

        scp $DPDEDUP_DIR/bin/* $USER@$host:$DPDEDUP_DIR/bin/
    } &
    done

    wait
fi

if [[ $MODE = "config" ]]
then
    for((i=1;i<=$NODE_NUM;i++))
    do
    {
        if [[ $i -gt 0 && $i -lt 10 ]]
        then
            host=${NODE_NAME}0${i}
        else
            host=${NODE_NAME}$i
        fi
        ip=${ip_list[$i-1]}
        scp $DPDEDUP_DIR/conf/* $USER@$host:$DPDEDUP_DIR/conf/
        ssh $USER@$host "sed -i \"s/local_ip ${ip_list[2]}/local_ip ${ip}/\" $DPDEDUP_DIR/conf/sys.conf"
    } &
    done

    wait
fi
