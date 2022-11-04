import sys
import itertools
from operator import add
from pyspark.sql import SparkSession
from collections import Counter

def map_friends_and_commons(user, friends):
    alreadyConnectedCount = -9999999999
    connecteds = [((user, friend), alreadyConnectedCount) for friend in friends]
    commons = [(pair, 1) for pair in itertools.permutations(friends, 2)]
    return connecteds + commons

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: friend-recommend <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonFriendRecommend")\
        .getOrCreate()

    numberOfRecommendations = 10

    rdd1 = spark.sparkContext.textFile(sys.argv[1])
    rdd2 = rdd1.map(lambda line: line.split('\t'))

    rdd3 = rdd2.flatMap(lambda x: map_friends_and_commons(x[0], x[1].split(',')))

    rdd4 = rdd3.reduceByKey(add)

    rdd5 = rdd4.filter(lambda x: x[1] > 0)
    rdd5.toDF().show()

    rdd6 = rdd5.map(lambda x: (x[0][0], x[0][1], x[1]))
    #rdd6.toDF().show()

    rdd7 = rdd6.filter(lambda x: x[0] == '1').sortBy(lambda x: x[2], ascending=False).toDF().show(numberOfRecommendations)

    spark.stop()
