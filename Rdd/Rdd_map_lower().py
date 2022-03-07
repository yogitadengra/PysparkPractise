from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

lower_words=words.map(lambda word:(word.lower(),1)).collect()
print(lower_words,"\n")
lower_words2=words.map(lambda word:(word.lower(),1)).take(6)
print(lower_words2)
