#Return a dataset with number of partition specified in the argument. This operation reshuffles the RDD randamly,
# It could either return lesser or more partioned RDD based on the input supplied.
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

partition=words.getNumPartitions()
print(partition)

result=words.coalesce(2).getNumPartitions()
print(result)

repart=words.repartition(10).getNumPartitions()
print(repart)
