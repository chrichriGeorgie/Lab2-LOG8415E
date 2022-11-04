#Building Spark Container
cd spark-wordcount
docker build -t spark-log8415 .

#Starting hadoop container
cd ..
docker run -d --name hadoop-log8415 --mount type=bind,source="$PWD"/resources,target=/opt/hadoop/resources hadoop-log8415

#Starting spark container
docker run -d --name spark-log8415 --mount type=bind,source="$PWD"/resources,target=/opt/hadoop/resources spark-log8415

#Experiment header
echo "HADOOP VS SPARK"
echo "======================================"
printf "\n"

docker exec -i hadoop-log8415 bash -c "echo Hello from Hadoop"
docker exec -i spark-log8415 bash -c "echo Hello from Spark"


#Stopping and removing containers
docker stop hadoop-log8415
docker stop spark-log8415
docker rm hadoop-log8415
docker rm spark-log8415