from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()

#lets take a df and add and substract five days from today.
#These functions take a column and then number of days to either add or substract as arguments.

from pyspark.sql.functions import date_add,date_sub
from pyspark.sql.functions import current_date, current_timestamp,col
df=spark.range(10)

df1= df.withColumn("today",current_date()).withColumn("now",current_timestamp())
df1.createOrReplaceTempView("table")
df1.printSchema()
df1.show(1)

df1.select(date_sub(col("today"),5),date_add(col("today"),5)).show(1)
#date_sub(subtract todays date), date_add(opposite) date_sub( , )