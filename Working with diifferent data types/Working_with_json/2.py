from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/Datanet.csv")
df.createOrReplaceTempView("dfTable")
df.show(5)

#You can also turn struct tye into json string by using to_json func

from pyspark.sql.functions import to_json,col,from_json
df.selectExpr("(Age,Name) as mystruct").select(to_json(col("mystruct")))

#you can usimg from_json to parse this(or from other data back in)
from pyspark.sql.types import *
parseschema=StructType((StructField("Age",StringType(),True),StructField("Name",StringType(),True)))
df.selectExpr("(Age,Name) as mystruct").select(to_json(col("mystruct")).alias("new"))\
.select(from_json(col("new"),parseschema),col("new")).show(5)