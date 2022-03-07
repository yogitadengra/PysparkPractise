from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

partition=words.getNumPartitions()
print(partition,"\n")

words_count=words.count()
print(words_count,"\n")

myrange=sc.parallelize(range(9),1)
result=words.zip(myrange).collect()
print(result)