import sys
from pyspark.sql import SparkSession, Row
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
	childs = matchingRows.select('destination').rdd.map(lambda x: x.destination)
	return (childs)

def noMatchFound(knownpaths):
	print("no match found")
	emptyPath = sqlContext.createDataFrame(sc.emptyRDD(), knownpaths.schema)
	return (emptyPath)

def isRowExisting(df, val_n):
	exists = df.where((df.node == val_n)).rdd.count()
	if exists > 0:
		return True
	else:
		return False


def traverse_graph_for_destintion(n,s,d, knownpaths):
	if(d == 6):
		return(noMatchFound(knownpaths))

	childs = getChildForNode(n)
	if (childs.count() > 0):
			for c in childs.collect():
				if isRowExisting(knownpaths, c) == False:
					knownpaths = insertRow(c , n, d +1 , knownpaths)
					# TODO: we need to show intermediate output here
					if(c == dest_node):
						# print('match found')
						return(knownpaths)

			for c in childs.collect():
				return(traverse_graph_for_destintion(c, n, d+1, knownpaths))
	else:
		return(noMatchFound(knownpaths))

def get_source_for_node(val, resulted_df, l_output):
	result_row = resulted_df.where(resulted_df.node == val).select("source")
	result = result_row.rdd.flatMap(list).first()
	l_output.append(result)
	if(result != source_node):
		return(get_source_for_node(result, resulted_df, l_output))

	return(l_output)



def main():
		
	knownpaths = sqlContext.createDataFrame(sc.emptyRDD(), schema)
	knownpaths = traverse_graph_for_destintion(source_node, source_node, 0, knownpaths)
	knownpaths.show()

	if len (knownpaths.take(1)) != 0 :
		l = get_source_for_node(dest_node, knownpaths, [dest_node])
		l.reverse()
		print(l)

if __name__ == "__main__":
	
	if(source_node == dest_node):
		print("source and destination are the same")
		exit()

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

	main()
