from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sampledata6.csv")
df.createOrReplaceTempView("dfTable")
df.show()
from pyspark.sql.functions import instr,col
#The INSTR function returns the index of the first occurrence of a substring in a string.
#Boolean expressions with multiple parts when you and or or.
#Boolean expressions are expressed serially(one after other)

#Spark flatten all of these filters into one statement
# and perform the filter at the same time.
st = col("id")>1005
nd = instr(df.City,"Qutubullapur") >=2
df.where(df.Sports.isin("Tennis")).where(st | nd).show(8)