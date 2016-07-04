#This script will find the maximum temperature recorded at a station 
#Data Format
'''
Station Code, Date, Ttype, Temp,,,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
'''

from pyspark import SparkConf, SparkContext
import collections

#Set master node- local, create spark context object.
conf = SparkConf().setMaster("local").setAppName("MaxTemp")
sc = SparkContext(conf = conf)

#Read data from the text file line by line and create and RDD with each line as a string in the list
lines = sc.textFile("file:///SparkCourse/datasets/1800.csv")

#Map input data
def parseLines(lines):
	lines = lines.split(',')
	station = lines[0]
	tType = lines[2]
	temp = float(lines[3])*0.1
	return(station,tType,temp)

parsedLines = lines.map(parseLines)

#Filtering out min temps
maxTemps = parsedLines.filter(lambda x:x[1] == 'TMAX')

#(station code, temp) key value pairs
stationTemps = maxTemps.map(lambda x: (x[0],x[2]))

#Find minimum temperature by station id
maxTemps = stationTemps.reduceByKey(lambda x,y:max(x,y))

#Convert results to python list
results = maxTemps.collect()

for result in results:
	print 'Station ID: '+result[0]+' Maximum Temperature: '+'{:.2f}C'.format(result[1])