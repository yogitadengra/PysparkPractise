from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()

#We can sort the df by using sort and orderBy
#if operating on column you need to use asc and desc


df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")

df.sort("order_id").show(5)
df.sort("order_id").show(5)
df.orderBy("order_id","order_cid").show(5)

from pyspark.sql.functions import desc,asc,col,expr
df.orderBy(expr("order_id desc")).show()
df.orderBy(col("order_cid").desc()).show(5)