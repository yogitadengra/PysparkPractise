from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
from pyspark.sql.functions import col
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/null.txt")
df.show()

#using fill function you can fill column with set of values
#For ex=fill all null values with same string gif column is string and fill int value if column is int or doubles
df.na.fill(67846).show(5)
#df.na.fill("null was here").show(5)

#we can give string or int valueto fill null for particular column whose value is null

df.na.fill(65,subset=["Age","Salary"]).show()




