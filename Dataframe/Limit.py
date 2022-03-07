from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()

#you want to ristrict num of columns, ex you might want just top 2 of some df
#you then use limit=it take 1 parameter


df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
df.limit(2).show()
collectdf = df.limit(5)

collectdf.take(5)
collectdf.show()
collectdf.show(5,False)
collectdf.collect()