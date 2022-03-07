#BOOLEAN IS CONDITIONAL REQUIREMENTS FOR WHEN A ROW OF DATA MUST EITHER PASS THE TEST OR ELSE
# IT WILL FILTERED OUT.
# IT CONSIST OF FOUR ELEMENTS-AND OR TRUE FALSE
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("D:/workspace/pyspark_ex/resouces/sampledata6.csv")
df.createOrReplaceTempView("dfTable")
df.show()
from pyspark.sql.functions import col,expr

 #select id,city from df where id!=1002;
#.show(n=20, truncate=True, vertical=False)
df.where(col("id")!=1002).select("id","City").show(5,False)
df.where("id>1002").show(7,False)
df.where(expr("City") == "Qutubullapur").show(6,False)
