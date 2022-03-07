from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sampledata6.csv")
df.createOrReplaceTempView("dfTable")
df.show()

#We can convert a list of values into a set of arguments and pass them into a function
#we use language feature called varargs
#using this function inravel an array of arbitrary lengrh and pass it as arguments to a function.
#this coupled with select makes it possible to create arbitrary numbers of column dynamically,

from pyspark.sql.functions import expr , locate
sample = ["Cricket","Badminton","Football"]
def sports_locator (column,colorstring):
    return locate(colorstring.upper(),column).cast("boolean").alias("is_")
selcol=[sports_locator(df.Sports, c) for c in sample]
selcol.append(expr("*"))
df.select(*selcol).where(expr("Cricket")).select("Sports").show(3,False)