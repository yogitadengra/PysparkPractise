from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
from pyspark.sql.functions import col

#drop null or fill them with a value
#Coalesce
#By coalesce you can select the first non null value from a set of columns
#If there are no null values it will returnthe first column.
from pyspark.sql.functions import coalesce
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/null.txt")
df.show()

df.select(coalesce(col("Salary"),col("Name"))).show()