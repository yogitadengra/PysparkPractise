#filter() transformation is used to filter the records in an RDD.
# In our example we are filtering all words starts with “a”.

from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
rdd = spark.sparkContext.textFile("D:/workspace/pyspark_ex/resouces/justdata.txt")
for element in rdd.collect():
    print(element)

rdd2=rdd.flatMap(lambda x: x.split(" "))
rdd3=rdd2.map(lambda x: (x,1))


rdd4=rdd3.reduceByKey(lambda a,b: a+b)
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()

rdd6 = rdd5.filter(lambda x : 'a' in x[1]).collect()
print(rdd6)
