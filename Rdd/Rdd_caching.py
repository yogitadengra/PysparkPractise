from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)


mycol1=("Spark Sheila Sabby The Definitive Guide : Big Data").split(" ")
words=sc.parallelize(mycol1)
#cache()=caches the data
a=words.cache()
print(a)

b=words.getStorageLevel()
print(b)