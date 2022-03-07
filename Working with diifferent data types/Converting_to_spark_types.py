from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sampledata6.csv")
df.createOrReplaceTempView("dfTable")
#lit function converts a type in spark,like here converted a couple pf different kinds of python to its corresponding spark

df.printSchema()
from pyspark.sql.functions import lit
df.select(lit(5), lit("five"),lit(5.0)).show()