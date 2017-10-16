import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, sum, max
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.context import SQLContext

spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
 

inputs = sys.argv[1]
output = sys.argv[2]
source_node = sys.argv[3]
dest_node = sys.argv[4]

distance = 0

def get_graphedges(line):
    list1 = line.split(':')
    if list1[1] == '':
        return None
    else:
    	s = list1[0]

    list2 = list1[1].split(' ')
    list2 = filter(None, list2)
    results = []
    for d in list2:
        results.append((s, d))

    return (results)

def insertRow(n,s,d, knownpaths):
	#insert a new row
	newRow = KnownRow(n,s,d)
	newDF = sqlContext.createDataFrame([newRow], schema=schema)
	knownpaths = knownpaths.union(newDF)
	return knownpaths

def getChildForNode(n):
	matchingRows = graphedges.where(graphedges['source'] == n)
	# matchingRows.select('destination').show()
	childs = matchingRows.select('destination').rdd.map(lambda x: x.destination)
	return (childs)

def noMatchFound(knownpaths):
	print("no match found")
	emptyPath = sqlContext.createDataFrame(sc.emptyRDD(), knownpaths.schema)
	return (emptyPath)

def foo(n,s,d, knownpaths):
	knownpaths = insertRow(n,s,d, knownpaths)
	print(n,s,d)
	knownpaths.show()

	if(d == 6): # only 6 iterations for now
		return(noMatchFound(knownpaths))

	if n == dest_node:
		print("match")
		return (knownpaths)
	else:
		childs = getChildForNode(n)
		if (childs.count() > 0):
			for c in childs.collect():
				return(foo(c, n, d+1, knownpaths))
		else:
			return(noMatchFound(knownpaths))


textinput = sc.textFile(inputs)
graphedges_rdd = textinput.map(lambda line: get_graphedges(line)).filter(lambda x: x is not None).flatMap(lambda x: x)#.coalesce(1)

graphedges = graphedges_rdd.toDF(['source', 'destination']).cache()
graphedges.show()

KnownRow = Row('node', 'source', 'distance')

schema = StructType([
StructField('node', StringType(), False),
StructField('source', StringType(), False),
StructField('distance', IntegerType(), False),
])

# initial_row = KnownRow(source_node, source_node, 0)
knownpaths = sqlContext.createDataFrame(sc.emptyRDD(), schema)
knownpaths = foo(source_node, source_node, 0, knownpaths)

knownpaths.show()






