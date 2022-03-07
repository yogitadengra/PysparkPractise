from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql import Row
from pyspark.sql.functions import col
#Df is immutable,you cannot append to df,To append you must union the oiginal df along with new df.
df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
schema = df.schema
newrow = [Row("13", "new009947","5876","null"),Row("14","new87654", "1987","null")]

rows = spark.sparkContext.parallelize(newrow)
newdf = spark.createDataFrame(rows, schema).show()
df.union(newdf).where("order_id= 2").where(col("order_status")!= "CLOSED").show()




