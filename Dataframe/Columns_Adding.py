from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql.functions import col,column ,expr,lit

df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
#formal way of adding column is .withColumn
df.withColumn("id",expr("order_id==order_cid")).show(2)
df1=df.withColumn("This is yoi",expr("order_id"))

#Adding column by (`) backtick (reserved character/keyword)
df1.selectExpr("`This is yoi`","`This is yoi` as `new col`").show(2)
df1.select(expr("`This is yoi`")).columns
df1.show(5)

