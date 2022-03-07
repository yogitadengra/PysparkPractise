from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
from pyspark.sql.functions import col
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/null.txt")
df.show()
df.na.replace([""],["88"],"Salary").show()
#replace all null value in certain column with given value