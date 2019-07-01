#!/usr/bin/env bash


#1. Setup Spark

# Setup Scala
sudo apt-get install scala -y

# Setup Python
#sudo apt-get install python3 -y

# Setup git
sudo apt-get install git -y

# set soft link to python
sudo ln -s /usr/bin/python3 /usr/bin/python

# get Spark
echo "downloading SPARK"
export SPARK_FILE=spark-2.4.3-bin-hadoop2.7.tgz
export SPARK_VERSION=spark-2.4.3-bin-hadoop2.7
export SPARK_URL=http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz
export SPARK_PATH=/usr/local/

# download spark
wget $SPARK_URL
sudo tar xvf $SPARK_FILE -C $SPARK_PATH
cd $SPARK_PATH

# set soft link to Spark
sudo ln -s $SPARK_VERSION spark

# make entry for .bashrc
echo "export SPARK_HOME=/usr/local/spark" >>~/.bashrc
#source ~/.bashrc
#echo "export PATH=$SPARK_HOME/bin:$PATH" >>~/.bashrc

#2. Setup spark on each node

#3. Setup Master slave configuration



