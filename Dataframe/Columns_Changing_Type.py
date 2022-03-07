from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql.functions import col,column ,expr,lit

df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
#convert one type to another
#ex=string type to integers
#lets convert our int order_cid column to long

df1=df.withColumn("customerid",col("order_id").cast("long"))
df1.printSchema()
