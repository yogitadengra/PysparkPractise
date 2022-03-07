from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sample3.csv")
df.createOrReplaceTempView("dfTable")
df.show()

#Adding or removing spaces around a string
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim

df.select(ltrim(lit("     hell   ")).alias("ltrim"),trim(lit("     hell   ")).alias("trim"),
lpad(lit("yogita"),8,"*").alias("lpad"),rpad(lit("yogita"),8,"*").alias("rpad")).show()