from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()

#to_date func allows you to convert string to date,optionally with specified value
from pyspark.sql.functions import to_date,lit,col
spark.range(5).withColumn("date",lit("2017-01-01")).select(to_date(col("date"))).show()

#Spark will not throw error if it cannot parse the date,it will just return null,
# this will be tricky in larger pipelines
#Lets take a look at the date format that has switched from year-today-day to year-day-month.
#spark will fail and return null.

spark.range(5).withColumn("date",lit("2017-01-01")).select(to_date(col("date"))).show()


#Lets fix this pipeline in next py file

