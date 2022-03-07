
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sampledata6.csv")
df.createOrReplaceTempView("dfTable")
df.show()

from pyspark.sql import regexp_extract ,col,instr
#pulling out given string from group of string
extractstring="(Nets|Celtics)"
df.select(regexp_extract(col("Team"),extractstring,1).alias("rand"),col("Team")).show(5)


#Sometimes rather than extracting we simply want to check for their existence
#We can do this with contains method on each column.
#This will return a boolean
containsnets=instr(col("Team"),"Nets")<=1
containsceltics=instr(col("Team"),"Celtics")<=1
df.withColumn("rando",containsnets|containsceltics).where("rando").select("Team").show()

#trial with just two values,but it becomes more complicated when there are values

