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

def addfunc(left,right):
    return left+right
from functools import reduce
result=kvchar.groupByKey().map(lambda row :(row[0],reduce(addfunc,row[1]))).collect()
for i in result: print(i)
