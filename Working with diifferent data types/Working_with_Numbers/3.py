from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sample3.csv")
df.createOrReplaceTempView("dfTable")
df.show()


#Another task to compute summary statistics for a column or set of columns.
#we can use describe function to archieve exactly this
#This will take all numeric columns and calculate the count,mean,standard deviation, min and max
df.describe().show()

colname = "Number"
quantileProbs = [0.5]
rel = 0.05
df.stat.approxQuantile("Number",quantileProbs ,rel)
#you can calculate approximate quantiles of your data using approxQuantile

