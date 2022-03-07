from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

chars=words.flatMap(lambda word: word.lower())

kvchar=chars.map(lambda letter: (letter,1))
print(kvchar.take(5))

result=kvchar.countByKey()
print(result)




