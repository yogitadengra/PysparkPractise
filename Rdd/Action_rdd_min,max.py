
from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

minimum_val=sc.parallelize(range(1,20)).max()
print(minimum_val,"\n")

maximum_val=sc.parallelize(range(0, 20)).min()
print(maximum_val)