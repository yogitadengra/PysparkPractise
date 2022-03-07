from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()

#Another task is to look the difference between two dates.
#We can do this with datediff function that will return the number of days in between two dates.
#when you want number of months between two dates then use function months_between

from pyspark.sql.functions import datediff , months_between , to_date,lit , to_timestamp
from pyspark.sql.functions import current_date, current_timestamp,col,date_add,date_sub
df=spark.range(10)

df1= df.withColumn("today",current_date()).withColumn("now",current_timestamp())
df1.createOrReplaceTempView("table")
df1.printSchema()
df1.show(1)
df1.withColumn("week_ago" , date_sub(col("today"),7))\
    .select(datediff(col("week_ago"),col("today"))).show(1)

df1.select(to_date(lit("2016-01-01")).alias("start"),to_date(lit("2017-05-22"))\
           .alias("end")).select(months_between(col("start"),col("end"))).show(3)