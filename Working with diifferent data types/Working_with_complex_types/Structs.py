from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/Datanet.csv")
df.createOrReplaceTempView("dfTable")
df.show(5)

#struct is dataframe within dataframe
df.selectExpr("struct(Number ,Age) as complex " ,"*").show(5)

#df with column complex
from pyspark.sql.functions import struct,col,expr
complexdf=df.select(struct("Name","Age").alias("complex"))
complexdf.createOrReplaceTempView("complexdf")
complexdf.show()

#We can query it by sing column getField
complexdf.select("complex.Name").show(4)
complexdf.select(col("complex").getField("Age")).show(4)


#We can query all values in struct by using
complexdf.select("complex.*").show() # *=all



