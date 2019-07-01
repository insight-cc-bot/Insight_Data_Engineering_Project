#!/usr/bin/env bash

# Setting java environment in JDK
# update package
sudo apt update

# Setup JAVA8
sudo apt install openjdk-8-jdk -y

cd ~
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> .bashrc
#source .bashrc
#echo "export PATH=$PATH:$JAVA_HOME/bin" >> .bashrc
#source .bashrc


echo "java home $JAVA_HOME"
echo "java path $PATH"
echo "java version $(java -version)"


# Setup SPARK

# Setup PostgreSQL

# Setup KAFKA

#export PACKAGES = ""


# install other package
#sudo apt install boto3

#TODO: 1. check scp from  EC2 to local
# scp -i ~/.ssh/Amanpreet-Kaur-IAM-keypair.pem ec2-54-189-124-211.us-west-2.compute.amazonaws.com:/home/ubuntu/bash_config.txt ~/Desktop/
