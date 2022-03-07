from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/Datanet.csv")
df.createOrReplaceTempView("dfTable")
df.show(5)

#ARRAY_CONTAINS
#We can see whether this array contains a value
from pyspark.sql.functions import array_contains,split,col
df.select(array_contains(split(col("Team")," "),"Celtics")).show()
# give boolean value

#EXPLODE
#explode function take a column that consists of arrays and creates one row(with the rest of values duplicated)per value in array
from pyspark.sql.functions import explode

df.withColumn("splitted" ,split(col("Team")," ")).withColumn("exploded" ,explode(col("splitted")))\
.select("Team"  ,"splitted", "exploded").show()





