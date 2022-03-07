
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('yoidata').getOrCreate()
#Creating dataframe from rdd.
columns = ["Name","Age"]
data = [("Yoi", "23"), ("Smriti", "24"), ("Rahul", "25")]
rdd = spark.sparkContext.parallelize(data)
df1=rdd.toDF(columns)  #pass column in parameter of df
#converting rdd to df
df1.show()
#showing your data, it take parameter if you want
df1.printSchema()
#print schema of data

#Creating dataframe from Sparksession
#it take rdd object as argument and chain with toDF() to specify name to the columns.

df2 = spark.createDataFrame(rdd).toDF(*columns) # * will merge column without any error
df2.show() #By default show() method displays only 20 rows from PySpark DataFrame






