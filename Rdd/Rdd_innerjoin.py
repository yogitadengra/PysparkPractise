from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

chars=words.flatMap(lambda word: word.lower())

kvchar=chars.map(lambda letter: (letter,1))
print(kvchar.take(5),"\n")


import random
dist = words.flatMap(lambda word : word.lower()).distinct()
keychars=dist.map(lambda c:(c,random.random()))
outpartitions=10
result=kvchar.join(keychars).count()
print(result)
result2=kvchar.join(keychars,outpartitions).count()
print(result)
