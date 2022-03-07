from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/Datanet.csv")
df.createOrReplaceTempView("dfTable")
df.show(5)

#maps are created by using map func,you then can select them
from pyspark.sql.functions import create_map,col
df.select(create_map(col("Team"),col("Age")).alias("comp")).show()


#you can query it bt using proper key,a missing key return null
df.select(create_map(col("Team"),col("Age")).alias("comp"),"Age").selectExpr("comp['Boston Celtics']").show()

#you can explode map types,which will turn them into columns
df.select(create_map(col("Name"), col("Weight")).alias("comp"))\
.selectExpr("explode(comp)").show()