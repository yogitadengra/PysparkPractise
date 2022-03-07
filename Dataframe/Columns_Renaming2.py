from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql.functions import col,column,expr

df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
#RENAMING COLUMNS

#alias / .withColumnRenamed
import pyspark.sql.functions as F
import pyspark.sql.types as T

df.select(F.col("order_id").alias("orderid"),F.col("order_d").alias("date")).show(4)
df.select("order_id","order_d").withColumnRenamed("orderid","date").show(4)

df.withColumnRenamed("order_id","emp_id").show()
