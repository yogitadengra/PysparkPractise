from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

supdata = {"Spark" : 1000 , "Definitive" : 200 , "Big" : 300 }
broadcast = sc.broadcast(supdata)
print(broadcast.value)


mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

result=words.map(lambda word : (word , broadcast.value.get(word , 0))).sortBy(lambda wordPair: wordPair[1]).collect()
for i in result: print(i)