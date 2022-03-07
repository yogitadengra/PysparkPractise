from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycoll= ("Spark The Definitive Guide : Big Data").split(" ")
words=sc.parallelize(mycoll)

words2=words.map(lambda word:(word,word[0],word.startswith("S"))).collect()
print(words2,"\n")

mycol1=("Spark Sheila Sabby The Definitive Guide : Big Data").split(" ")
words3=sc.parallelize(mycol1)
words4=words3.map(lambda word:(word,word[0],word.startswith("S"))).collect()
print(words4)

#map() transformation is used the apply any complex operations like adding a column, updating a column e.t.c,
# the output of map transformations would always have the same number of records as input.

