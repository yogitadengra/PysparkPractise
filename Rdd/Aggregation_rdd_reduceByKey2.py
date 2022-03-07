from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
rdd = spark.sparkContext.textFile("D:/workspace/pyspark_ex/resouces/justdata.txt")
for element in rdd.collect():
    print(element)

rdd2=rdd.flatMap(lambda x: x.split(" "))
rdd3=rdd2.map(lambda x: (x,1))


rdd4=rdd3.reduceByKey(lambda a,b: a+b).collect()
for i in rdd4: print(i)

#reduceByKey() merges the values for each key with the function specified.
# In our example, it reduces the word string by applying the sum function on value.
# The result of our RDD contains unique words and their count.