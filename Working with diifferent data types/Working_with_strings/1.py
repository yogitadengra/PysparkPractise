from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sample3.csv")
df.createOrReplaceTempView("dfTable")
df.show()
from pyspark.sql.functions import initcap,expr,lower,upper

#The initcap function will capitalize every word in a given string when the word is seperated from another by a space
df.select(initcap(expr("Name"))).show(2)

#cast string in uppercase and lowercase
df.select(expr("College"),
          lower(expr("College")),
          upper(lower(expr("College")))).show()




