# LOG8415E - Assignment 2
# friend-recommend.py
# Python file to get friend recommandations of different users.
# This file is inspired by https://github.com/xjlin0/cs246/blob/master/w2015/hw1/q1_people_you_may_know_spark.py

import sys
import itertools
from operator import add
from pyspark.sql import SparkSession
import time

# Map operation to do a pair of friends and common friends.
# A negative value is given to friends, since we want to remove them after
# while the common friends will have a value of 1 for 1 pair.
def map_friends_and_commons(user, friends):
    alreadyConnectedCount = -9999999999
    alreadyFriends = [((user, friend), alreadyConnectedCount) for friend in friends]
    mutuals = [(pair, 1) for pair in itertools.permutations(friends, 2)]
    return alreadyFriends + mutuals

# Combine the pair of common friends count.
# Returns an array with the user, the potential friend and the amount of mutual friends.
def map_user_potential_friend_count(row):
    user = row[0][0]
    potentialFriend = int(row[0][1])
    count = row[1]
    return (user, potentialFriend, count)

# Find the 10 friend recommandations for a specific user.
def get_user_recommendations(dataFrame, user):
    numberOfRecommendations = 10
    potentialFriendsString = "\t"

    dataFrame.createOrReplaceTempView("table")
    rows = spark.sql("select user, potentialFriend from table where user == " + user + " ORDER BY count desc, potentialFriend asc LIMIT " + str(numberOfRecommendations)).collect()
    if(len(rows) > 0):
        for row in rows:
            potentialFriendsString += str(row[1]) + ","

    print(user + potentialFriendsString[:-1])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: friend-recommend <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonFriendRecommend")\
        .getOrCreate()
    spark._sc.setLogLevel('WARN')

    startTime = time.time()

    # Get the resource data of user-friends and read it line by line.
    lines = spark.sparkContext.textFile(sys.argv[1])
    userFriends = lines.map(lambda line: line.split('\t'))

    # Map-Reduce operation to have the count of pair of common friends.
    # Remove already friends pairs.
    pairCounts = userFriends.flatMap(lambda x: map_friends_and_commons(x[0], x[1].split(','))) \
    .reduceByKey(add) \
    .filter(lambda row: row[1] > 0)

    userPotentialFriend = pairCounts.map(lambda x: map_user_potential_friend_count(x)).toDF(['user', 'potentialFriend', 'count'])

    # Prints the friend recommandations for different users.
    get_user_recommendations(userPotentialFriend, '924')
    get_user_recommendations(userPotentialFriend, '8941')
    get_user_recommendations(userPotentialFriend, '8942')
    get_user_recommendations(userPotentialFriend, '9019')
    get_user_recommendations(userPotentialFriend, '9020')
    get_user_recommendations(userPotentialFriend, '9021')
    get_user_recommendations(userPotentialFriend, '9022')
    get_user_recommendations(userPotentialFriend, '9990')
    get_user_recommendations(userPotentialFriend, '9992')
    get_user_recommendations(userPotentialFriend, '9993')

    endTime = time.time()
    print("Execution took ", endTime-startTime, " sec")

    spark.stop()
