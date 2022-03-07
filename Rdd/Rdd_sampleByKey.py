from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

import random
distinctchar=words.flatMap(lambda word:list(word.lower())).distinct().collect()
print(distinctchar)

samplemap=dict(map(lambda c: (c,random.random()), distinctchar))
result=words.map(lambda word: (word.lower()[0], word)).sampleByKey(True,samplemap,6).collect()
print(result)

#Samplebykey
#sample(withReplacement, fraction, seed=None)
#fraction – Fraction of rows to generate, range [0.0, 1.0]. Note that it doesn’t guarantee to provide the exact number of the fraction of records.
#seed – Seed for sampling (default a random seed). Used to reproduce the same random sampling.
#withReplacement – Sample with replacement or not (default False).



