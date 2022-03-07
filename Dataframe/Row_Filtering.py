from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql import Row
from pyspark.sql.functions import col
#we can ue filter and where to filter out the rows,can put multiple filters into same expression
df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
df.show()
df.filter(col("order_cid")<10000).show()
df.filter("order_cid<1000").show()
df.where(col("order_cid")<100).where(col("order_status")=="COMPLETE").show()

df.where(col("order_cid")>1000).where(col("order_status")!="COMPLETE").show()
