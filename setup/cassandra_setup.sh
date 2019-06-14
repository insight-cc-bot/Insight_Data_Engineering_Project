# Configure a Multi-Node Cassandra Cluster
# ref: https://www.digitalocean.com/community/tutorials/how-to-configure-a-multi-node-cluster-with-cassandra-on-a-ubuntu-vps

# 1. Install Cassandra on each node
# ref: https://www.digitalocean.com/community/tutorials/how-to-install-cassandra-and-run-a-single-node-cluster-on-a-ubuntu-vps

cd ~
export CASSANDRA_VERSION=http://mirrors.ocf.berkeley.edu/apache/cassandra/3.11.4/apache-cassandra-3.11.4-bin.tar.gz
export CASSANDRA_FILE=apache-cassandra-3.11.4

# installing Cassandra on a node
wget $CASSANDRA_VERSION

tar -xzvf "${CASSANDRA_FILE}-bin.tar.gz"

mv $CASSANDRA_FILE /usr/local/cassandra

cd /usr/local/cassandra

# set soft link
sudo ln -s cassandra cassandra


# Cassandra has the right to write on it:
sudo mkdir /var/lib/cassandra
sudo mkdir /var/log/cassandra
sudo chown -R $USER:$GROUP /var/lib/cassandra
sudo chown -R $USER:$GROUP /var/log/cassandra

# set cassandra variables 
export CASSANDRA_HOME=/usr/local/cassandra
export PATH=$PATH:$CASSANDRA_HOME/bin

# execute cassandra script
sudo sh bin/cassandra


# 2. Set configuration on each node
# Ensure Cassandra not running
sudo ps auwx | grep cassandra
sudo kill -9 PID

# Configure Cassandra.yaml on each node - 
vi ~/cassandra/conf/cassandra.yaml


# add following to the file

# Node 0
cluster_name: 'InsightCassandra'
initial_token: 0
seed_provider:
    - seeds:  "Root IP" 
listen_address: "RootID"
rpc_address: 0.0.0.0
endpoint_snitch: RackInferringSnitch

# Node 1
cluster_name: 'InsightCassandra'
initial_token: 3074457345618258602
seed_provider:
    - seeds:  "Root IP" 
listen_address: "Node 2 IP"
rpc_address: 0.0.0.0
endpoint_snitch: RackInferringSnitch

# Node 2
cluster_name: 'InsightCassandra'
initial_token: 6148914691236517205
seed_provider:
    - seeds:  "Root IP" 
listen_address: "Node 3 IP"
rpc_address: 0.0.0.0
endpoint_snitch: RackInferringSnitch


# set
listen_address = self address
rpc_address: self address
# Error 
# ERROR [main] 2019-06-14 04:45:15,770 CassandraDaemon.java:749 - Exception encountered during startup: The number of initial tokens (by initial_token) specified is different from num_tokens value

# Generating tokens in Cassandra
# https://docs.datastax.com/en/archived/cassandra/2.0/cassandra/configuration/configGenTokens_c.html