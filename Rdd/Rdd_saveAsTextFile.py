from pyspark import SparkContext , SparkConf
conf = SparkConf().setAppName("Data").setMaster("local")
sc = SparkContext(conf=conf)


mycol1=("Spark Sheila Sabby The Definitive Guide : Big Data").split(" ")
words=sc.parallelize(mycol1)

savedfile=words.saveAsTextFile("/workspace/pyspark_ex/saving files/words.txt")
savedfile


