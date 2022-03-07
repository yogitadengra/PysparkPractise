from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycoll= ("Spark The Definitive Guide : Big Data").split(" ")
words=sc.parallelize(mycoll)
a=words.flatMap(lambda word: list(word)).take(8)
print(a)

#flatmap()=Returns flattern map meaning if you have a dataset with array,
# it converts each elements in a array as a row.
# In other words it return 0 or more items in output for each element in dataset.

