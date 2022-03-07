from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
udfex = spark.range(5).toDF("num")
udfex.show()

def add(value):
    return value+3
add(2)

from pyspark.sql.functions import udf , col,expr
from pyspark.sql.types import IntegerType , DoubleType
addudf = udf(add)
udfex.select(addudf(col("num"))).show()
spark.udf.register("add",add,DoubleType())
udfex.selectExpr("add(num)").show()

spark.udf.register("addpy",add,DoubleType())
udfex.selectExpr("addpy(num)").show