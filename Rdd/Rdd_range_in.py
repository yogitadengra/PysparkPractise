#range always return in row
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rd").getOrCreate()


#we want numbers in column wise then use map
#map is transformation that returns new rdd
#collect() don't take any parameter, it is operation of rdd that retrieve data from df
spark.range(10).toDF("id").rdd.map(lambda row: row[0]).collect()

range_rdd=spark.range(10).rdd
for i in range_rdd.take(10): print(i)

spark.range(10).rdd.toDF().show()



