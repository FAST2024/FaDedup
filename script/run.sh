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

# compile
make -p ../build
make -p ../bin
cd ../build
cmake ..
make

# dispatch
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

    scp $DPDEDUP_DIR/bin/DPAgent $USER@$host:$DPDEDUP_DIR/bin/
    scp $DPDEDUP_DIR/bin/DPClient $USER@$host:$DPDEDUP_DIR/bin/
} &
done

# start DPAgent
for((i=1;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		host=${NODE_NAME}$i
	fi

    ssh $USER@$host "cd $DPDEDUP_DIR; ./bin/DPAgent"
} &
done

wait


