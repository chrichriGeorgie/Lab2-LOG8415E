import sys
import itertools
import re
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

    rdd1 = spark.sparkContext.textFile(sys.argv[1])
    rdd2 = rdd1.map(lambda line: line.split('\t'))
    #rdd2.toDF().show()

    rdd3 = rdd2.flatMap(lambda x: map_friends_and_commons(x[0], x[1].split(',')))
    #rdd3.toDF().show()

    rdd4 = rdd3.reduceByKey(lambda total, current: total + current)
    #rdd4.toDF().show()

    rdd5 = rdd4.filter(lambda x: x[1] > 0)
    rdd5.toDF().show()

    rdd6 = rdd5.map(lambda x: (x[0][1], (x[1], x[0][1])))
    rdd6.toDF().show()

    spark.stop()
