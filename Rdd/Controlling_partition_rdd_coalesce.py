#Similar to repartition by operates better when we want to the decrease the partitions. Betterment acheives
# by reshuffling the data from fewer nodes compared with all nodes by repartition.
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
