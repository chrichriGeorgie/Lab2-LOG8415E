#Building Spark Container
cd spark-wordcount
docker build -t spark-log8415 .

#Starting hadoop container
cd ..
docker run -d --name hadoop-log8415 --mount type=bind,source="$PWD"/resources,target=/opt/hadoop/resources hadoop-log8415

#Starting spark container
docker run -d --name spark-log8415 --mount type=bind,source="$PWD"/resources,target=/opt/hadoop/resources spark-log8415

#Experiment header
printf "\n"
echo "HADOOP VS SPARK"
echo "======================================"
printf "\n"

#Setting input for Hadoop
docker exec -d hadoop-log8415 bash -c "hdfs dfs -rm input/pg4300.txt"
docker exec -d hadoop-log8415 bash -c "hdfs dfs -copyFromLocal resources/hadoop-spark-dataset/* input"

#Initialising total time variables
hadooptotal=0
sparktotal=0

#Executing the experiment 3 times
for i in 1 2 3
do
    #Iteration header
    printf "\n"
    echo "ITERATION $i"
    echo "--------------------------------------"
    #TODO run all files 3 times for each input, get all times in array, print all time in a table, mesure & print average

    #Running experiment with Hadoop
    docker exec -d hadoop-log8415 bash -c 'if [ -d "output" ]; then hdfs dfs -rm -r output; fi;'
    echo "Starting Wordcount on Hadoop..."
    printf "\n"
    hadooptime=$( TIMEFORMAT="%R"; { time docker exec -i hadoop-log8415 bash -c "hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.1.jar wordcount input output" > hadoop-run.txt; } 2>&1 )
    echo "Hadoop Execution Time: $hadooptime"
    printf "\n"

    #Running experiment with Spark
    echo "Starting Wordcount on Spark..."
    printf "\n"
    sparktime=$( TIMEFORMAT="%R"; { time docker exec -i spark-log8415 bash -c '/opt/spark/bin/spark-submit wordcount.py /opt/hadoop/resources/hadoop-spark-dataset/' > spark-output.txt; } 2>&1 )
    echo "Spark Execution Time: $sparktime"
    printf "\n"

    #Incrementing the hadoop and spark total time to calculate the average
    hadooptotal=$(echo $hadooptotal + $hadooptime | bc)
    sparktotal=$(echo $sparktotal + $sparktime | bc)  
done

#Retrieving Hadoop last Output to make sure everything went right
docker exec -i hadoop-log8415 bash -c "cat output/*" > hadoop-output.txt

#Calculating and printing Hadoop and Spark time average
hadoopavg=$(echo $hadooptotal / 3 | bc -l)
printf '\nHadoop average execution time: %0.3f \n' "$hadoopavg"
sparkavg=$(echo $sparktotal / 3 | bc -l)
printf 'Spark average execution time: %0.3f \n\n' "$sparkavg"

#Stopping and removing containers
docker stop hadoop-log8415
docker stop spark-log8415
docker rm hadoop-log8415
docker rm spark-log8415