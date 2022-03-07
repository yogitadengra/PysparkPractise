from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql.functions import col,column ,expr,lit

df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
#df.drop() ,can take more than 1 parameter
df1=df.withColumn("yoi",expr("order_id"))
df1.selectExpr("`yoi`","`yoi` as `new col`")
df1.select(expr("`yoi`")).columns

df1.drop("yoi").columns



