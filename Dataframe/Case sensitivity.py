from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
#Spark is case insensitive
#set spark.sql.caseSensitive true