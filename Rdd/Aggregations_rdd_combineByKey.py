from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

mycollection="Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words=sc.parallelize(mycollection)
a=words.take(10)
print(a,"\n")

chars=words.flatMap(lambda word: word.lower())

kvchar=chars.map(lambda letter: (letter,1))
print(kvchar.take(5),"\n")
def val(value):
    return[value]
def mergev(vals,valtoappend):
    vals.append(valtoappend)
    return vals
def mergec(vals1,vals2):
    return vals1+vals2
outputpartition=6
result=kvchar.combineByKey(val,mergev,mergec,outputpartition).collect()
for i in result: print(i)

