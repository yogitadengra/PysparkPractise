from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
#literals=add constant value in new column

df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
from pyspark.sql.functions import lit
from pyspark.sql import expr
df.select(expr("*"),lit(2).alias("One")).show(2)
df.withColumn("o",lit(2)).show(2)