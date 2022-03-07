from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sampledata6.csv")
df.createOrReplaceTempView("dfTable")
df.show()

from pyspark.sql.functions import regexp_replace, translate, regexp_extract, instr,col
df.show()

#Another task might be to replace given characters with other language..
#This is done at the character level and will replace all instances of a character with the indexed character in replacement string.

df.select(translate(col("Sports"),"abcdef","123456"),col("Sports")).show()

df.select(translate(col("City"),"abcdefgh","12345678").alias("random"),col("City")).show()

