
from pyspark import SparkConf, SparkContext
import collections

#Set master node- local, create spark context object.
conf = SparkConf().setMaster("local").setAppName("TokenFreq")
sc = SparkContext(conf = conf)

#Read data from the text file line by line and create and RDD with each line as a string in the list
lines = sc.textFile("file:///SparkCourse/datasets/Book.txt")

#Tokenize Data using FlatMap, each word will have a unique entry in the RDD
#Can use regex to add complexity to split...
words = lines.flatMap(lambda x:x.split())

#Calculate word frequencies
wordFreq = words.countByValue()

for word, count in wordFreq.items():
	cleanWord = word.encode('ascii','ignore')
	if(cleanWord):
		print cleanWord,count