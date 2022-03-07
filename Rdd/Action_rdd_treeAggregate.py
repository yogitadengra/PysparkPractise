#treeAggregate() – Aggregates the elements of this RDD in a multi-level tree pattern. The output of this function will be similar to the aggregate function.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("Z", 1), ("A", 20), ("B", 30), ("C", 40), ("B", 30), ("B", 60)]
inputRDD = spark.sparkContext.parallelize(data)

listRdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 3, 2])


#treeAggregate. This is similar to aggregate
seqOp = (lambda x, y: x + y)
combOp = (lambda x, y: x + y)
agg=listRdd.treeAggregate(0, seqOp, combOp)
print(agg) # output 20
