#Rounding things
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sample3.csv")
df.createOrReplaceTempView("dfTable")
df.show()
from pyspark.sql.functions import lit,round,bround,corr

#round is a function for rounding integer to a whole number
#In this ex we round to one decimal place

df.select(round(lit("2.5")),bround(lit("2.5"))).show(5)
#round give approx nearest value, bround(opposite)

#Another numeric task is to compute the correlation of two columns
#ex=we can see the pearson correlation for two columns if cheaper things are typically bought in greater quantities.
#we can do this through a func as well as through the dataframe
df.stat.corr("Number","Weight")
df.select(corr("Number","Weight")).show()




