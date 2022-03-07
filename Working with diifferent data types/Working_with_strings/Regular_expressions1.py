from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sampledata6.csv")
df.createOrReplaceTempView("dfTable")
df.show()

from pyspark.sql.functions import regexp_replace, translate, regexp_extract, instr,col
df.show()

#One of the most frequently performed tasks is searching for the existence of one sting in another
#Or replacing all mentions of a sting with another value. This is done by a tool called regular expressions
#Regular exressions give user an ability to specify a set of rules to use to either extract values
#from a sting or to replace them with some other values

#There are two key func in spark that you will need in order to perform regular expressions
#regex_extract and regex_replace to extract and replace values.

reg="Cricket|Badminton|Football"
df.select(regexp_replace(col("Sports"),reg,"Sports").alias("colo"),col("Sports")).show()

df.select(regexp_replace(col("Sports"),reg,"Sports").alias("colo")).show()






