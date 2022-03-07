from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql.functions import col,column

df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")

#SELECTING COLUMNS

#The .select transformation allows us to extract one or more column from df
df.select("order_id","order_d").show(3)
df.select(col("order_d"),col("order_d"),column("order_status")).show(2)
df.select(col("order_id"),"order_d").show(2)


#show() is action to have at the end
df.select("order_id","order_d").explain()



