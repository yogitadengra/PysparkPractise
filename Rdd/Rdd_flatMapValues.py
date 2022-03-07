from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

keyword=words.keyBy(lambda word:word.lower()[0])
result=keyword.flatMapValues(lambda word: word.upper()).collect()
for i in result: print(i)
