#!/bin/sh
hdfs dfs -mkdir input
hdfs dfs -copyFromLocal resources/pg4300.txt input
sleep 50000
