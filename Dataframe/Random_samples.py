from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql import Row
from pyspark.sql.functions import col

#Sometimes you want to sample some random records from df, you do it by using sample method.
df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
seed=3
ft=True
gf=0.12
df.sample(ft,gf,seed).show()