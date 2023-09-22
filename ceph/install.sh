NODE_NUM=3
NODE_NAME=node
USER="$USER"
HOME=/home/$USER
DPDEDUP_DIR=$HOME/DPDedup

# install base dependencies on each node
for((i=0;i<=$NODE_NUM;i++));
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		if [[ $i -eq 0 ]]
		then
			host=admin
		else
			host=${NODE_NAME}$i
		fi
	fi

    ssh $USER@$host "sudo apt install -y openssh-server git net-tools systemd ntp lvm2 curl make cmake librados-dev libradosstriper-dev python3-pip ufw python-netaddr"

	ssh $USER@$host "sudo ufw disable"
} &
done 

wait

# copy DPDedup source code to each node
for((i=1;i<=$NODE_NUM;i++))
do
{
	if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		if [[ $i -eq 0 ]]
		then
			host=admin
		else
			host=${NODE_NAME}$i
		fi
	fi

	scp -r $DPDEDUP_DIR $USER@$host:$HOME
} &
done
wait

# install sockpp on each node
for((i=0;i<=$NODE_NUM;i++));
do
{
	if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		if [[ $i -eq 0 ]]
		then
			host=admin
		else
			host=${NODE_NAME}$i
		fi
	fi

	ssh $USER@$host "cd $DPDEDUP_DIR/ceph; tar -zxvf sockpp.tar.gz"
	ssh $USER@$host "cd $DPDEDUP_DIR/ceph/sockpp; cmake -Bbuild .; cmake --build build/; sudo cmake --build build/ --target install"
} &
done
wait

# install openssl on each node
for((i=0;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		if [[ $i -eq 0 ]]
		then
			host=admin
		else
			host=${NODE_NAME}$i
		fi
	fi

	ssh $USER@$host "cd $DPDEDUP_DIR/ceph; tar -zxvf openssl-1.0.2d.tar.gz"
	ssh $USER@$host "cd $DPDEDUP_DIR/ceph/openssl-1.0.2d; ./config --prefix=/usr/local --openssldir=/usr/local/openssl "
	ssh $USER@$host "cd $DPDEDUP_DIR/ceph/openssl-1.0.2d; make && sudo make install"
	ssh $USER@$host "sudo cp -r /usr/local/include/openssl/ /usr/include/"
} &
done 

# install redis in all nodes
for((i=0;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		if [[ $i -eq 0 ]]
		then
			host=admin
		else
			host=${NODE_NAME}$i
		fi
	fi

	ssh $USER@$host "cd $DPDEDUP_DIR/; tar -zxvf redis-3.2.8.tar.gz"
	ssh $USER@$host "cd $DPDEDUP_DIR/redis-3.2.8/; make && sudo make install"
	ssh $USER@$host "cd $DPDEDUP_DIR/redis-3.2.8/utils/; sudo ./install_server.sh"
	ssh $USER@$host "sudo service redis_6379 stop; sudo sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/6379.conf; sudo sed -i 's/protected-mode yes/protected-mode no/' /etc/redis/6379.conf; sudo service redis_6379 start"
	ssh $USER@$host "sudo service redis_6379 stop; sudo sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/redis.conf; sudo sed -i 's/protected-mode yes/protected-mode no/' /etc/redis/redis.conf; sudo service redis_6379 start"
} &
done 


# install hiredis in all nodes
for((i=0;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		if [[ $i -eq 0 ]]
		then
			host=admin
		else
			host=${NODE_NAME}$i
		fi
	fi

	ssh $USER@$host "cd $DPDEDUP_DIR/; tar -zxvf hiredis-1.0.2.tar.gz"
	ssh $USER@$host "cd $DPDEDUP_DIR/hiredis-1.0.2/; make && sudo make install"
	ssh $USER@$host "sudo cp /usr/local/lib/libhiredis.* /usr/lib/"
	ssh $USER@$host "sudo cp /usr/local/lib/libssl.a /usr/lib; sudo cp /usr/local/lib/libcrypto.a /usr/lib"
} &
done

wait


# install ceph dependencies on admin node
for((i=0;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		if [[ $i -eq 0 ]]
		then
			host=admin
		else
			host=${NODE_NAME}$i
		fi
	fi

	ssh $USER@$host "sudo -H pip3 install -U pip; sudo -H pip3 install pecan werkzeug pyyaml"
} &
done

wait

cd $DPDEDUP_DIR/ceph

unzip v5.0.5.zip

cd ceph-ansible-5.0.5

sudo -H pip3 install -r requirements.txt

cp $DPDEDUP_DIR/ceph/ansible/all.yml $DPDEDUP_DIR/ceph/ceph-ansible-5.0.5/group_vars/
cp $DPDEDUP_DIR/ceph/ansible/osds.yml $DPDEDUP_DIR/ceph/ceph-ansible-5.0.5/group_vars/
cp $DPDEDUP_DIR/ceph/ansible/hosts.yml $DPDEDUP_DIR/ceph/ceph-ansible-5.0.5/
cp $DPDEDUP_DIR/ceph/ansible/site.yml $DPDEDUP_DIR/ceph/ceph-ansible-5.0.5/

# install and deploy ceph
ansible-playbook -i hosts site.yml

git clone https://github.com/ceph/ceph.git

sudo cp -r ceph/src/include/* /usr/include/