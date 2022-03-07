from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql import Row
from pyspark.sql.functions import col

#GETTING UNIQUE COLUMNS=EXTRACT DISTINCT VALUES,THESE VALUES CAN BE IN ONE OR MORE COLUMNS.
#distinct()=doesn't take any parameter

df=spark.read.format("csv").option("header","True").load("D:/workspace/pyspark_ex/resouces/orders.txt")
df.select("order_id","order_cid").distinct().show()
count=df.select("order_id").distinct().count()
print(count)