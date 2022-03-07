#counting things
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sample3.csv")
df.createOrReplaceTempView("dfTable")
df.show()
from pyspark.sql.functions import col,expr

#pow function that raises a column to the expressed power:

fabricatedquality=pow(col("Weight")* col("Number"),2)+5   #180*0+5=5
df.select(expr("Height"),fabricatedquality.alias("file_output")).show(5)
#We were able to multiply our columns together because they were both numerical.
#Naturally we can add and substract ,infact we can do all of this as a SQL.

df.selectExpr("Number","(POWER((Weight * Number),2)+5)as result").show()
#Number=2,200*2(pow(2))+5=160005