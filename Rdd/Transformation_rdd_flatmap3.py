from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
rdd = spark.sparkContext.textFile("D:/workspace/pyspark_ex/resouces/justdata.txt")

for element in rdd.collect():
    print(element)

rdd2=rdd.flatMap(lambda x: x.split(" ")).collect()
print(rdd2)
#or can use for loop

