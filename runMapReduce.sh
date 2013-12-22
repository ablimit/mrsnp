#!/bin/bash

export HADOOP_HOME=/usr/lib/hadoop-0.20-mapreduce
echo "Hadoop home directory is: $HADOOP_HOME" 
sudo -u hdfs hdfs dfs -rmr /user/xsun28/output


echo ""
echo "Start executing genome profiles package ..."
echo ""

START=$(date +%s)
sudo -u hdfs hadoop jar ./genome.jar Query1 /user/xsun28/input  /user/xsun28/output 1 93 0.001 93834
END=$(date +%s)
DIFF=$(( $END - $START ))

echo "Total execution time is: $DIFF" 

