from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycoll= ("Spark The Definitive Guide : Big Data").split(" ")
words=sc.parallelize(mycoll)
for i in words.collect(): print(i)

result=words.sortBy(lambda word: len(word)).take(7)
print("\n",result,"\n")

result1=words.sortBy(lambda word: len(word)+ -1).take(7)
print(result1)