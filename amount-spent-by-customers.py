#This script will find the total amount spent by customers with a particular id
#Data Format
#CustomerID, ProductID,Amt Spent
#85,1733,28.53
#Expected output
#CustomerID, Amt Spent


from pyspark import SparkConf, SparkContext

#Set master node- local, create spark context object.
conf = SparkConf().setMaster("local").setAppName("CustomerAmounts")
sc = SparkContext(conf = conf)

#Read data from the text file line by line and create and RDD with each line as a string in the list
lines = sc.textFile("file:///SparkCourse/datasets/customer-orders.csv")

def parseLines(lines):
	lines = lines.split(',')
	return (int(lines[0]),float(lines[2]))

reqData = lines.map(parseLines)

amtSpent = reqData.reduceByKey(lambda x,y: x+y).sortByKey()

results = amtSpent.collect()

for k,v in results:
	k=str(k)
	print "Customer ID: "+k+"\tAmount Spent: "+'{:.2f}'.format(v)

