from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
seed=3
df1=df.randomSplit([0.25,0.75],seed)
df2=df1[0].count()>df1[1].count()
print(df2)

