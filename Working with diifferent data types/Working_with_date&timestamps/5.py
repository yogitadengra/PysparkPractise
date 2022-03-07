from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
#To fix this ,we will use two functions to fix to_date and to_timestamp

from pyspark.sql.functions import to_date,lit,col,to_timestamp
dateformat = "yyyy-dd-mm"
cleandatadf = spark.range(1).select(to_date(lit("2017-12-11"), dateformat)\
                              .alias("date"),to_date(lit("2017-20-12"), dateformat).alias("date2"))

cleandatadf.createOrReplaceTempView("datetable")
cleandatadf.show()

cleandatadf.select(to_timestamp(col("date"),dateformat)).show()
#if we are comparing a date
cleandatadf.filter(col("date2") > "`2017-12-12`").show()