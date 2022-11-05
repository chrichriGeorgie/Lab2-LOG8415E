#!/bin/bash
#setting working directory
cd "$(dirname "$0")"

echo "Closing and removing existing container"
docker container stop spark
docker container rm spark

echo "Building image"
docker build . -t spark-friendship

echo "Running container"
cd ..
docker run -d -it --mount type=bind,source="$(pwd)"/resources,target=/opt/spark/work-dir/resources --name spark spark-friendship
docker exec -it spark /opt/spark/bin/spark-submit friend-recommend.py resources/friendship-dataset/soc-LiveJournal1Adj.txt