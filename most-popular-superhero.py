#This script will find the most popular superhero based on a social graph of superheroes from the Marvel Universe!
from pyspark import SparkConf, SparkContext

#Set master node- local, create spark context object.
conf = SparkConf().setMaster("local").setAppName("PopularSuperhero")
sc = SparkContext(conf = conf)

#Read data from the text file line by line and create and RDD with each line as a string in the list


def parseNames(line):
	fields = line.split('\"')
	return (int(fields[0]),fields[1].encode("utf8"))


def numberOfCoOccurences(line):
	elements = line.split()
	return (int(elements[0]),len(elements)-1)


names = sc.textFile("file:///SparkCourse/datasets/marvel-names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("file:///SparkCourse/datasets/marvel-graph.txt")
occurenceCount = lines.map(numberOfCoOccurences)

totalFriendsByCharacter = occurenceCount.reduceByKey(lambda x,y:x+y)
flipped = totalFriendsByCharacter.map(lambda (x,y):(y,x))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print mostPopularName + " is the most popular superhero in the marvel universe!"

