from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)
rdd = sc.parallelize((0,20))
print("yoi"+str(rdd.getNumPartitions()))
print(rdd.getNumPartitions())
#we use str because string added to string only
#getnumpartition dont take any parameter

rdd1 = sc.parallelize((0,25), 6)
print("parallelize : "+str(rdd1.getNumPartitions()))
rdd.saveAsTextFile("D:/workspace/pyspark_ex/saving files/y.txt")
