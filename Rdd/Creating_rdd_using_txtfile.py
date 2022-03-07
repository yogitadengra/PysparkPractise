from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

# Create RDD from external Data source
rdd2 = spark.sparkContext.textFile("D:/workspace/pyspark_ex/resouces/orders.txt")
print(rdd2.collect())