#we use map here, we are adding a new column with value 1 for each word,
# the result of the RDD is PairRDDFunctions which contains key-value pairs,
# word of type String as Key and 1 of type Int as value

from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
rdd = spark.sparkContext.textFile("D:/workspace/pyspark_ex/resouces/justdata.txt")
for element in rdd.collect():
    print(element)

rdd2=rdd.flatMap(lambda x: x.split(" "))
rdd3=rdd2.map(lambda x: (x,1)).collect()
for i in rdd3: print(i)

rdd4=rdd.map(lambda x: (x,2)).collect()
for i in rdd4: print(i)