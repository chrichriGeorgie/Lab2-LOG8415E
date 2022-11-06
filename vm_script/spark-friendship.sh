#!/bin/bash
#setting working directory
cd spark-friendship

#Stoping and removing spark friendship containers and images
echo "Closing and removing existing container"
docker stop spark
docker rm spark

#Build spark friendship container
echo "Building image"
docker build . -t spark-friendship

#Start spark friendship container and run the algorithm
echo "Running container"
cd ..
docker run -d -it --mount type=bind,source="$(pwd)"/resources,target=/opt/spark/work-dir/resources --name spark spark-friendship
docker exec -it spark /opt/spark/bin/spark-submit friend-recommend.py resources/friendship-dataset/soc-LiveJournal1Adj.txt

#Stoping and removing spark friendship containers and images
echo "Closing and removing existing container"
docker stop spark
docker rm spark