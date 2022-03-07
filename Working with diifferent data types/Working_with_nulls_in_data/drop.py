from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
from pyspark.sql.functions import col
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/null.txt")
df.show()

#The simple func is drop, which removes rows that contain nulls.
#The default is to drop any row in which any value is null.
#df.na.drop() or df.na.drop(any) or df.na.drop(all)
#any=drop row if any of values is null
#all=drop row if all values are null

#can apply to certain sets of column
df1=df.na.drop("all",subset=["Age"])
df1.show()




