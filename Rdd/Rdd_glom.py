from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

num=sc.parallelize([5,5,4,3,2,9,2]).glom().collect()
print(num)

result=sc.parallelize(["hello","world"],2).glom().collect()
print(result)
