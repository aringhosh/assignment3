import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, sum, max

spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
 

inputs = sys.argv[1]
# output = sys.argv[2]



def main():
    # do things...
    schema = types.StructType([
    types.StructField('ID', types.StringType(), False),
    types.StructField('DATE', types.LongType(), False),
    types.StructField('TYPE', types.StringType(), False),
    types.StructField('VALUE1', types.IntegerType(), False),
    types.StructField('MFlag', types.StringType(), True),
    types.StructField('QFlag', types.StringType(), True)
    ])

    t = spark.read.csv(inputs, schema=schema)
    
    t.createOrReplaceTempView('MASTER')
    t = spark.sql("""
    SELECT ID, DATE, (2 * MAX(VALUE1) - SUM(VALUE1)) AS RANGE
    FROM MASTER
    WHERE (QFlag IS NOT NULL) AND (TYPE ='TMAX' OR TYPE = 'TMIN')
    GROUP BY DATE, ID
""")
    t.createOrReplaceTempView('MASTER2')

    p = spark.sql("""
    SELECT DATE, MAX(RANGE) AS MAX_RANGE
    FROM MASTER2
    GROUP BY DATE
    SORT BY DATE ASC
""")

    p.show()
 
if __name__ == "__main__":
    main()