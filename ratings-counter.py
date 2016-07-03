# This script will parse the ratings dataset and return a histogram of ratings, 1-5

from pyspark import SparkConf, SparkContext
import collections

#Set master node- local, create spark context object.
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#Read data from the text file line by line and create and RDD with each line as a string in the list
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

#Map values of lines RDD to ratings RDD by splitting each line and isolating index [2], movie ratings
ratings = lines.map(lambda x: x.split()[2])

#Count rating results
result = ratings.countByValue()

#Sort and print results 
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    print "Rating: %s Number of movies:%i" % (key, value)
 
