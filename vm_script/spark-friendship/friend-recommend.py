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

def map_user_potential_friend_count(row):
    user = row[0][0]
    potentialFriend = row[0][1]
    count = row[1]
    return (user, potentialFriend, count)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: friend-recommend <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonFriendRecommend")\
        .getOrCreate()

    numberOfRecommendations = 10

    lines = spark.sparkContext.textFile(sys.argv[1])
    userFriends = lines.map(lambda line: line.split('\t'))

    pairCounts = userFriends.flatMap(lambda x: map_friends_and_commons(x[0], x[1].split(','))) \
    .reduceByKey(add) \
    .filter(lambda row: row[1] > 0)

    userPotentialFriend = pairCounts.map(lambda x: map_user_potential_friend_count(x))

    rdd7 = userPotentialFriend.filter(lambda x: x[0] == '1').sortBy(lambda x: x[2], ascending=False).toDF().show(numberOfRecommendations)

    spark.stop()
