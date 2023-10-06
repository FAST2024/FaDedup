# FaDedup

Here is the source code of FaDedup, which is a prototype system designed for distributed primary storage systems coupled with local deduplication. 

Next you will see the instructions for installing and runnig FaDedup.

## Installation

We developed FaDedup on Ubuntu 20.04. 

### Ceph

We take a four-node cluster as an example to illustrate how to deploy a cephalopods cluster by Ceph-Ansible, which is a official tool offered by Ceph to deploy ceph clusters automaticly.

We prepare four machines: node01-node04

First, we can use `apt` to install the prerequisites on all the machines.

```shell
apt update
apt install -y openssh-server git net-tools systemd ntp lvm2 curl make cmake librados-dev libradosstriper-dev python3-pip ufw python-netaddr
```

Second, we use the `tzselect` tool to synchronize the clocks of the four machines.

```shell
tzselect
```

Third, we select a machine as the machine responsible for installation and ensure that it can log in to other machines via SSH without a password.

```shell
ssh-keygen -t rsa

ssh-copy-id node01 node02 node03 node04
```

Fourth, we install some extra requried tools by `pip`

```shell
sudo -H pip3 install -U pip

sudo -H pip3 install pecan werkzeug pyyaml
```

Fifth, we download the ceph-ansible and install the requirements.

```shell
wget https://github.com/ceph/ceph-ansible/archive/refs/tags/v5.0.5.zip

unzip v5.0.5.zip

sudo -H pip3 install -r requirements.txt

cd ceph-ansible-5.0.5
```

Then we modify the yaml files as follows:

all.yml

---
```yaml
dummy:

mon_group_name: mons
osd_group_name: osds
mds_group_name: mdss
mgr_group_name: mgrs
grafana_server_group_name: grafana-server

configure_firewall: False

ceph_origin: repository
ceph_repository: community
ceph_mirror: https://mirrors.tuna.tsinghua.edu.cn/ceph
ceph_stable_key: https://mirrors.tuna.tsinghua.edu.cn/ceph/keys/release.asc
ceph_stable_release: octopus
ceph_stable_repo: "{{ ceph_mirror }}/debian-{{ ceph_stable_release }}"
ip_version: ipv4
monitor_interface: ens33
public_network: 192.168.235.0/24
cluster_network: 192.168.235.0/24
monitor_address_block: 192.168.235.0/24
cephx: true
copy_admin_key: true
osd_objectstore: bluestore
dashboard_enabled: false
```

osds.yml

The devices' names depend on the real name of the machines' devices.

---

```yaml
dummy:

devices:

  - /dev/sdb
  - /dev/sdc
```

We create a file called *hosts*

hosts

```yaml
[mons]
node01
node02
node03
node04

[osds]
node02
node03
node04

[mgrs]
node01
node02
node03
nofr04

[mdss]
node01
node02
node03
node04

[grafana-server]
node02
```

Install and deploy ceph via ceph-ansible.

```shell
ansible-playbook -i hosts site.yml

sudo ceph config set mon auth_allow_insecure_global_id_reclaim false
```

Download the ceph source code.

```shell
git clone https://github.com/ceph/ceph.git

sudo cp -r ceph/src/include/rados/ /usr/include/
```

### OpenSSL

Install OpenSSL.

```shell
tar -zxvf openssl-1.0.2d.tar.gz

cd openssl-1.0.2d

./config --prefix=/usr/local --openssldir=/usr/local/openssl

 make && make install

cp -r /usr/local/include/openssl/ /usr/include/
```

### Redis

Install Redis.

```shell
tar -zxvf redis-3.2.8.tar.gz

cd redis-3.2.8

make && make install

cd utils/

./install_server.sh

service redis_6379 stop

sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/6379.conf

sed -i 's/protected-mode yes/protected-mode no/' /etc/redis/6379.conf

service redis_6379 start

service redis_6379 stop

sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/redis.conf

sed -i 's/protected-mode yes/protected-mode no/' /etc/redis/redis.conf 

service redis_6379 start
```

Install hiredis

```shell
tar -zxvf hiredis-1.0.2.tar.gz

cd hiredis-1.0.2

make && make install

cp /usr/local/lib/libhiredis.* /usr/lib/

cp /usr/local/lib/libssl.a /usr/lib

cp /usr/local/lib/libcrypto.a /usr/lib
```

## Compile

After preparing the development environment above, we can compile FaDedup via `cmake` easily.

```shell
mkdir build

cd build

cmake ..

make
```

## Preparation

### Configuration

There is a configuration file in the *conf* folder.

#### sys.conf

The file describes the system configuration info. The keys and values are seperated by a whitespace. Each key occupies one line.

| Key                 | Description                                                  |
| ------------------- | ------------------------------------------------------------ |
| ceph_conf_path      | The path of the ceph configuration file.                     |
| local_ip            | The IP address of the local node.                            |
| agents_ip           | The IP addresses of all nodes, including the local node. Addressses are seperated by a whitespace. |
| packet_size         | A file will first be split into coarse-grained packets. Packet_size is the size of packets in bytes. |
| agent_worker_number | Each agent has multiple workers which can response the users' requests in parallel. Agent_worker_number represents the number of workers. |
| read_thread_num     | The number of read requests that FaDedup can handle in parallel. |
| chunk_avg_size      | The average size of chunks.                                  |
| chunk_algorithm     | The chunking algorithm.                                      |
| hash_algorithm      | The fingerprinting algorithm.                                |
| container_size      | The number of chunks read or written per I/O                 |
| super_chunk_size    | The number of chunks within a superchunk.                    |
| write_mode          | The mode used when writing data.There are eight write mode can be chosen: `Normal`, `NormalMulti`, `SelectiveDedup`, `SelectiveDedupMulti`, `SuperChunk`, `SuperChunkMulti`,  `BatchPush`, `BatchPushMulti`.                                                |
| update_mode         | The mode used when updating data. There are two update mode can be chosen: `Normal`, `Remap`.                                                       |

## Run

### Start
After the compilation is completed, run *start_agent.sh* in *script* folder on one node or run *DPAgent* obtained after compilation at each node.
```shell
# run on one node
bash script/start_agents.sh
# or run on each node
./DPAgent
```

### Write

FaDedup supports six write modes which can be configured in *conf/sys.conf*.(i) `Normal`, means normal write strategy; (ii) `SelectiveDedup`, means writing with FSR strategy; (iii) `BatchPush` means writing with batch strategy; (iv) `SuperChunk` means writing with superchunk strategy; (v) `*Multi` means writing files in parallel with corresponding strategy. 

After `DPAgent` has been successfully started on all nodes, run `DPClient` on a node to perform write operations.

If the write mode is set to `Normal`/`SelectiveDedup`/`BatchPush`/`SuperChunk`, which means writing one file at a time, use the following command to write:

Usage:
```shell
./DPClient write [filename] [saveas] test online [sizeinByte] 
```
Example:
```shell
./DPClient write /path/to/file name test online 268435456
```
We show the usage and a command-line example
to write a file (file path is */path/to/file*) with size 268435456 bytes and store it as *name*. `test` is the pool name which the file is to be written and `test` is the default pool name. `online` is the dedup mode and `online` is the default mode.

If the write mode is set to `NormalMulti`/`SelectiveDedupMulti`/`BatchPushMulti`/`SuperChunkMulti`,  which means writing files in parallel at the same time, use the following command to write:

Usage:
```shell
./DPClient write [jobname]
```

`jobname` is a file path where multiple write tasks are written to. Each line of this file is like a command writing a single file. The contents of a file represented by `jobname` may be as follows:

```
write /path/to/file1 file1 test online 268435456
write /path/to/file2 file2 test online 268435456
write /path/to/file3 file3 test online 268435456
```

### Read
After write operations are completed, use the following command to read one file:

Usage:
```shell
./DPClient read [filename] test [saveas]
```
Example:
```shell
./DPClient read name test localname
```
`filename` is the name set when writing this file, i.e., `saveas` in write command. `test` is the pool name and `test` is the default pool name. `saveas`is name of the file to be read and saved locally.

### Reorder
After write operations are completed, use the following command to perform reorder operation:

```shell
./DPClient startreorder
```

### Update
After write operations are completed, use the following command to perform update operation:

```shell
./DPClient update [jobname]
```

`jobname` is a file path where multiple update tasks are written to. The contents of a file represented by jobname may be as follows:

Usage:
```shell
update [filename0] test [offset0] [size0] [update content0]
update [filename1] test [offset1] [size1] [update content1] 
update [filename2] test [offset2] [size2] [update content2]  
```

Example:
```shell
update name1 test 0 4096 /path/to/update_cotent1
update name2 test 4096 4096 /path/to/update_content2
update name3 test 8192 4096 /path/to/update_content3
```
`filename` is the name set when writing this file. `offset` is the start position of the update operation. `size` is the size of update operation, `update content` is a file path where data to be written is located in this update operation. In the above example we update *name1*, *name2*, and *name3* with offset 0, 4096, and 8192 and size of 4096 bytes. The data of the threeupdate operations comes from file path */path/to/update_cotent1*, */path/to/update_cotent2*, */path/to/update_cotent3*.