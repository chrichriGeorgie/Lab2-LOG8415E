#!/bin/bash
#setting working directory
cd "$(dirname "$0")"
maind=$PWD

#starting hadoop container
cd hadoop-wordcount
docker build -t hadoop-log8415 .
docker run -d --name hadoop-log8415 --mount type=bind,source="$maind"/resources,target=/opt/hadoop/resources hadoop-log8415

#Run WordCount Experiment on Hadoop
echo "Running Hadoop WordCount on pg4300.txt"
echo "Time Taken:"
time docker exec -it hadoop-log8415 hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.1.jar wordcount input output > hadoop-output.txt
echo " "
echo "Results:"
docker exec -it hadoop-log8415 cat output/part-r-00000

#Run WordCount Experiment on Linux

echo "Running Linux WordCount on pg4300.txt"
echo "Time Taken:"
docker exec -it hadoop-log8415 touch linux-output.txt
time docker exec -it hadoop-log8415 cat ./resources/pg4300.txt | tr ' ' '\n' | sort | uniq -c #>> linux-output.txt

echo "Results:"
#docker exec -it hadoop-log8415 cat linux-output.txt

#Removing Hadoop container and image
echo " "
docker stop hadoop-log8415
docker rm hadoop-log8415
docker rmi -f hadoop-log8415