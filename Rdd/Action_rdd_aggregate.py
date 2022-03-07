#Aggregate lets you transform and combine the values of the RDD at will.
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)

def maxfunc(left,right):
    return max(left,right)

def addfunc(left,right):
    return left+right
nums=sc.parallelize(range(1,31),5)

result=nums.aggregate(0,maxfunc,addfunc)
print(result)

depth=3
result2=nums.treeAggregate(0,maxfunc,addfunc,depth)
print(result2)