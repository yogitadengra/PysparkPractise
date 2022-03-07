from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df=spark.read.option("delimiter","|").option("header", "true").csv("D:/workspace/pyspark_ex/resouces/population.txt")
lookup = dict(

    {"TX": "Texas",
	"NY" : "New York",
	 "OH" : "OHIO",
	"CA" : "Califonia",
	"IL" : "Illinois",
	"CO" : "Colorado",
    "AZ" : "Arizona"})
broad=spark.sparkContext.broadcast(lookup)
broad.value["NY"]

from pyspark.sql.functions import udf
from pyspark.sql.functions import *

def broadval(col):
    return broad.value[col]

funcreg=udf(broadval)

df.withColumn("state",funcreg("state_code")).show()
