#This scrpit will find the average number of friends for each age group
#Data Format
#ID,Name,Age,Number of Friends
#0,Will,33,385

from pyspark import SparkConf, SparkContext
import collections

#Set master node- local, create spark context object.
conf = SparkConf().setMaster("local").setAppName("FriendsByAversge")
sc = SparkContext(conf = conf)

#Read data from the text file line by line and create and RDD with each line as a string in the list
lines = sc.textFile("file:///SparkCourse/datasets/fakefriends.csv")

#Mapping the input data
#OP :: (Age,Number of Friends)

def parseLines(lines):
	lines = lines.split(',')
	age = int(lines[2])
	numberOfFriends = int(lines[3])
	return (age,numberOfFriends)

rdd = lines.map(parseLines)

#Count sum of friends and number of entries per age

#(Age,(Number of Friends,1))
totalByAge = rdd.mapValues(lambda x:(x,1))

#(Age,sum(Number of Friends, total))
totalByAge = totalByAge.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

#(Age,Average)
averageByAge = totalByAge.mapValues(lambda x:(x[0]/x[1]))

#Collect Results
results = averageByAge.collect()

#Print Results
for result in results:
	print result

