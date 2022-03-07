from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/Datanet.csv")
df.createOrReplaceTempView("dfTable")
df.show(5)
from pyspark.sql.functions import col

#SPLIT

#first task is to turn our Team column into complex type, an array
#we do this by using split function

from pyspark.sql.functions import split
df.select(split(col("Team"),"")).show()
#split(" ") =add ,("") =add , in each char

#Spark allows to manipulate this complex type as another column
#we can query this data using python like syntax

df.select(split(col("Name")," ").alias("array")).selectExpr("array[0]").show()

#ARRAY LENGTH
#determine array length
from pyspark.sql.functions import size
df.select(size(split(col("Age")," "))).show()

