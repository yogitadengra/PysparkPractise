from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

#Sometimes we may need to write an empty RDD to files by partition.
#Create empty RDD with partition
rdd = spark.sparkContext.parallelize([],10) #This creates 10 partitions
print(rdd.collect())
print(rdd.getNumPartitions())
