#filter() transformation is used to filter the records in an RDD.
from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycoll= ("Spark The Definitive Guide : Big Data").split(" ")
words=sc.parallelize(mycoll)
words.collect()

def startsWithG(individual):
    return individual.startswith("S")

a=words.filter(lambda word: startsWithG(word)).collect()
print(a)

b=words.filter(lambda record: record[0]).collect()
print(b)



