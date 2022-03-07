from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sampledata6.csv")
df.createOrReplaceTempView("dfTable")
df.show()

#Now we did not need to specify as an expression and how we could use a column name without any extra work

from pyspark.sql.functions import expr
df.withColumn("new",expr("id>1005")).where("new").select("City","new","id").show()

#when you deal with null data
#df.where(col("description").eqNullSafe("hello)).show()