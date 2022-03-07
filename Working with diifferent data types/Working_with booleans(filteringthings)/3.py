from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sampledata6.csv")
df.createOrReplaceTempView("dfTable")
df.show()
#Boolean expression are not just reserved words to filters.
#To filter a df, you can specify a boolean column as well.


from pyspark.sql.functions import col,instr
dot=col("Sports")=="Tennis"
ide=col("id")>1002
des=instr(col("LastName"), "Ball")>=1
df.withColumn("is",dot & (ide | des)).where("is").select("id","is").show(5)
# where sports==tennis and id>1002

dot=col("Sports")=="Tennis"
ide=col("id")<1008
des=instr(col("City"), "Neerharen")>=1
df.withColumn("is",dot & (des | ide)).where("is").select("id","is").show(5)

