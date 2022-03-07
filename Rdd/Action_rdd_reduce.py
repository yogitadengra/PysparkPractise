from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

result=sc.parallelize(range(1,21)).reduce(lambda x,y:x+y)
print(result)

mycol1=("Spark Sheila Sabby The Definitive Guide : Big Data").split(" ")
words=sc.parallelize(mycol1)

def wordlength(leftword,rightword):
    if len(leftword)>len(rightword):
        return leftword
    else:
        return rightword
output=words.reduce(wordlength)
print(output)

#reduce() – Reduces the elements of the dataset using the specified binary operator.
