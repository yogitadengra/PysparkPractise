from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

wordmap=words.map(lambda word: (word[0],word.lower())).collect()
print(wordmap)

keyword=words.keyBy(lambda word:word.lower()[0])
wordmapValues=keyword.mapValues(lambda word: word.upper())
for i in wordmapValues.collect(): print(i)