from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

rdd =spark.sparkContext.emptyRDD()
print(rdd.collect())

#Using emptyRDD() method on sparkContext we can create an RDD with no data.
# This method creates an empty RDD with no partition.