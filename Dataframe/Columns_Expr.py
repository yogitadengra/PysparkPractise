from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql.functions import col,column ,expr

#extracting columns
df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt").columns
print(df)
#or
#df.columns[:2]

#The most simple way to refer column is to use the .col function
reference=col("order_id")
print(reference)

reference2=expr("orde_id")
print(reference2)
#col=expr



