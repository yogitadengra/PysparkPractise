#sortByKey() transformation is used to sort RDD elements on key.
# In our example, first, we convert RDD[(String,Int]) to RDD[(Int,String]) using map transformation
# and later apply sortByKey which ideally does sort on an integer value.
# And finally, foreach with println statement prints all words in RDD and their count as key-value pair to console.

from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
rdd = spark.sparkContext.textFile("D:/workspace/pyspark_ex/resouces/justdata.txt")
for element in rdd.collect():
    print(element)

rdd2=rdd.flatMap(lambda x: x.split(" "))
rdd3=rdd2.map(lambda x: (x,1))


rdd4=rdd3.reduceByKey(lambda a,b: a+b)
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey().collect()
for i in rdd5: print(i)
