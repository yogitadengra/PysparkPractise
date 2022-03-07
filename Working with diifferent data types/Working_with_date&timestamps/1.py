#Dates focus on calendar dates
#timestamps focus on both date and time
#TimestampTypes class support only second level precision, if you work with mili or microseconds.

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()

#very basic , get current date and timestamp
from pyspark.sql.functions import current_date, current_timestamp
df=spark.range(10)

df1= df.withColumn("today",current_date()).withColumn("now",current_timestamp())
df1.createOrReplaceTempView("table")
df1.printSchema()
df1.show(1)
