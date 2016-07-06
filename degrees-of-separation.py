#Boilerplate code for a distributed implementation of BFS

'''
Steps::
1.Represent each line as a node with connections, a color and a distance.
	(nodeID,(Neighbouring node IDs),Distance,Color)
	EG: 5983 1165 3836 4361 1282-->(5983,(1165,3863,4361,1282),9999,WHITE) 
2.Write a map function to convert input lines to BFS nodes.
3.Iteratively process the RDD.
	>Iterate and look for gray nodes to expand.
	>Color explored nodes black.
	>Update distances.
4.The mapper-use flatMap:
	>Creates new nodes for each connection of gray nodes, with a distance incremented by one,color gray, and no connections.
	>Colors the gray nodes we just processed black.
	>Copies the node itself into the results.
5.The reducer:
	>Combines together all nodes for the same ID.
	>Preserves the shortest distance.
	>Preserves the list of connections from the original node. 
6.Break Condition:
	>Use accumulator. For each iteration, if the ID we are interested in is hit, increment the accumulator by 1.
	>Check hitCounter at every iteration, when >1, we can break.
'''

#Here We GO!!!!!!
#Spark Config...
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

#The nodes we wish to find the degree of separation between:
rootNodeID = 5306 #SpiderMan
targetNodeID = 14  #ADAM 

#Initializing an accumulator for distributed BFS traversal
hitCounter = sc.accumulator(0)

#Function to convert input data to BFS nodes
#I/P data format 5983 1165 3836 4361 1282-->(5983,(1165,3863,4361,1282),9999,WHITE)
def convertToNode(line):
	line = line.split(' ')
	nodeID = line[0]
	edges = []
	for edge in line[1:]:
		edges.append(int(edge))
	
	color = 'WHITE'
	distance = 9999

	if (nodeID == rootNodeID):
		color = 'GRAY'
		distance = 0

	return (nodeID,(edges,distance,color))
	
	



