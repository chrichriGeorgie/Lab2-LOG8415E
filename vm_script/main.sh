#!/bin/bash
#setting working directory
cd "$(dirname "$0")"

#Start hadoop vs linux experiment
./hadoop_vs_linux.sh

#Start hadoop vs spark experiment
./hadoop_vs_spark.sh

#Start Frienship problem
./spark-friendship.sh