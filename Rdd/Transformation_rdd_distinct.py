from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycoll= "Spark The Definitive Guide : Big Data".split(" ")

words=sc.parallelize(mycoll)
type(words)
#set another name in rdd
words.setName("myWords")
words.name()
words.distinct().count()

a=sc.parallelize([1,1,2,2,3,3])
a.distinct().count()


