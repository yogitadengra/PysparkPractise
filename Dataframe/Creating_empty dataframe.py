
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Yoidata').getOrCreate()

#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)


#Creates Empty RDD using parallelize
rdd2= spark.sparkContext.parallelize([])
print(rdd2)




