
from pyspark import SparkConf, SparkContext
import re

#Set master node- local, create spark context object.
conf = SparkConf().setMaster("local").setAppName("TokenFreq")
sc = SparkContext(conf = conf)

#Read data from the text file line by line and create and RDD with each line as a string in the list
lines = sc.textFile("file:///SparkCourse/datasets/Book.txt")

#Tokenize Data using FlatMap, each word will have a unique entry in the RDD
#Can use regex to add complexity to split...
def normalizeWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower())


words = lines.flatMap(normalizeWords)

#Calculate word frequencies
wordFreq = words.map(lambda x:(x,1))
wordFreq = wordFreq.reduceByKey(lambda x,y: x+y)
wordFreq = wordFreq.map(lambda (x,y):(y,x))
wordFreqSorted = wordFreq.sortByKey()

results = wordFreqSorted.collect()

for result in results:
	count = str(result[0])
	cleanWord = result[1].encode('ascii','ignore')
	if(cleanWord):
		print cleanWord+ ":\t\t" +count