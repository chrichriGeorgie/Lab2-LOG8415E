import sys
import itertools
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

def map_friends_and_commons(user, friends):
    alreadyConnectedCount = -9999999999
    connecteds = [((user, friend), alreadyConnectedCount) for friend in friends]
    commons = [(pair, 1) for pair in itertools.permutations(friends, 2)]
    return connecteds + commons

def map_user_potential_friend_count(row):
    user = row[0][0]
    potentialFriend = int(row[0][1])
    count = row[1]
    return (user, potentialFriend, count)


def get_user_recommendations(dataFrame, user, maxRecommendations):
    potentialFriendsString = "\t"

    dataFrame.createOrReplaceTempView("table")
    rows = spark.sql("select user, potentialFriend from table where user == " + user + " ORDER BY count desc, potentialFriend asc LIMIT " + str(maxRecommendations)).collect()
    if(len(rows) > 0):
        for row in rows:
            potentialFriendsString += str(row[1]) + " "

    print(user + potentialFriendsString)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: friend-recommend <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonFriendRecommend")\
        .getOrCreate()

    startTime = time.time()
    numberOfRecommendations = 10

    lines = spark.sparkContext.textFile(sys.argv[1])
    userFriends = lines.map(lambda line: line.split('\t'))
    pairCounts = userFriends.flatMap(lambda x: map_friends_and_commons(x[0], x[1].split(','))) \
    .reduceByKey(add) \
    .filter(lambda row: row[1] > 0)

    userPotentialFriend = pairCounts.map(lambda x: map_user_potential_friend_count(x)).toDF(['user', 'potentialFriend', 'count'])

    get_user_recommendations(userPotentialFriend, '924', numberOfRecommendations)
    get_user_recommendations(userPotentialFriend, '8941', numberOfRecommendations)
    get_user_recommendations(userPotentialFriend, '8942', numberOfRecommendations)
    get_user_recommendations(userPotentialFriend, '9019', numberOfRecommendations)
    get_user_recommendations(userPotentialFriend, '9020', numberOfRecommendations)
    get_user_recommendations(userPotentialFriend, '9021', numberOfRecommendations)
    get_user_recommendations(userPotentialFriend, '9022', numberOfRecommendations)
    get_user_recommendations(userPotentialFriend, '9990', numberOfRecommendations)
    get_user_recommendations(userPotentialFriend, '9992', numberOfRecommendations)
    get_user_recommendations(userPotentialFriend, '9993', numberOfRecommendations)

    endTime = time.time()
    print("Execution took ", endTime-startTime, " sec")

    spark.stop()
