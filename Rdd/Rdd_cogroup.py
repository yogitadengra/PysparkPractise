from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

import random
dist = words.flatMap(lambda word : word.lower()).distinct()
charr = dist.map(lambda c: (c , random.random()))
charr2 = dist.map(lambda c: (c , random.random()))
result=charr.cogroup(charr2)
for i in result.take(5): print(i)