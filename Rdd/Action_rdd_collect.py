#collect() -Return the complete dataset as an Array.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("Z", 1), ("A", 20), ("B", 30), ("C", 40), ("B", 30), ("B", 60)]
inputRDD = spark.sparkContext.parallelize(data)

listRdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 3, 2])


#Collect
data = listRdd.collect()
print(data)
