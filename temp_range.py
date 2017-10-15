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
    t = t.where(col("QFlag").isNull())
    # t.show()

    p = t.filter((col("TYPE") == "TMAX") | (col("TYPE") == "TMIN")) \
        .groupby('DATE', 'ID').agg( (2 * max("VALUE1") - sum("VALUE1")).alias("Range"))
        
    max_table = p.groupby('DATE').agg(max("Range").alias('MaxRange'))
    
    cond2 = [p['DATE'] == max_table['DATE'], p['Range'] == max_table['MaxRange']]
    df_result = p.join(max_table, cond2, 'inner').select(p['DATE'], p['ID'], p['Range']).sort(col("DATE"))

    df_result.show()

    # df_result.saveAsTextFile(output)
 
if __name__ == "__main__":
    main()