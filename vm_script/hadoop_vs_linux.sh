#Building hadoop container
cd hadoop-wordcount
docker build -t hadoop-log8415 .

#Starting hadoop container
cd ..
docker run -d --name hadoop-log8415 --mount type=bind,source="$PWD"/resources,target=/opt/hadoop/resources hadoop-log8415

#Experiment header
printf "\n"
echo "HADOOP VS LINUX"
echo "======================================"
printf "\n"

#Run WordCount Experiment on Hadoop
echo "Running Hadoop WordCount on pg4300.txt"
hadtime=$( TIMEFORMAT="%R"; { time docker exec -i hadoop-log8415 bash -c "hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.1.jar wordcount input output > hadoop-output.txt"; } 2>&1 )
echo "Time Taken: $hadtime"
printf "\n"

#Run WordCount Experiment on Linux

echo "Running Linux WordCount on pg4300.txt"
lintime=$( TIMEFORMAT="%R"; { time docker exec -i hadoop-log8415 bash -c "cat ./resources/pg4300.txt | tr ' ' '\n' | sort | uniq -c > linux-output.txt"; } 2>&1 )
echo "Time Taken: $lintime"
printf "\n"

#Stoping and removing hadoop vs linux containers and images
docker stop hadoop-log8415
docker rm hadoop-log8415
