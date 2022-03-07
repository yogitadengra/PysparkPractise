from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("app").getOrCreate()
df = spark.read.option("header","true").csv("D:/workspace/pyspark_ex/resouces/orders.txt")
df.show(4)
df.rdd.getNumPartitions()
df_coll = df.repartition(5)
df_coll.rdd.getNumPartitions()
df1=df_coll.coalesce(4)
df.repartition(3,"order_id").rdd.getNumPartitions()
from pyspark.sql.functions import spark_partition_id
df2=df.repartition(3,"order_id").withColumn("partitionid", spark_partition_id()).groupBy("partitionid").count()
df2.show()

df.write.partitionBy("order_id")
