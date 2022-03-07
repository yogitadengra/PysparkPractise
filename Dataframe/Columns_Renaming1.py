from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql.functions import col,column,expr

df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
#RENAMING COLUMNS
#with a long list of columns you would like to change only few column names.
#This is very useful when joining dataframe with duplicate column names.

#selectExpr /AS /select(expr)/alias

df.select(expr("order_id as iid")).show(1)
df.select(expr("order_d as date").alias("id")).show(1)
df.select(expr("order_id as id").alias("city")).show(1)
df.selectExpr("order_status as empid","order_cid").show(1)
df.selectExpr("*","(order_id=order_cid) as with").show(3) #false because id is not equals to city.
df.selectExpr("avg(order_id)","count(distinct(order_cid))").show(3)

df.show()