from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('rdd').getOrCreate()

df=spark.read.option("header","true").option("interSchema","true").csv("D:/workspace/pyspark_ex/resouces/orders.txt")
rdd=df.coalesce(10).rdd
schema=df.printSchema()
print(schema)

partition=rdd.getNumPartitions()
print(partition)

