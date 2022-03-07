from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('rdd').getOrCreate()

df=spark.read.option("header","true").option("interSchema","true").csv("D:/workspace/pyspark_ex/resouces/orders.txt")
rdd=df.coalesce(10).rdd


keyrdd = rdd.keyBy(lambda row : row[3])
result=keyrdd.collect()
for i in result: print(i)


def partifunc(key):
    import random
    if key == 17850 or key == 12583:
        return 0
    else:
        return random.randint(1, 2)

a=keyrdd.partitionBy(3,partifunc).take(5)
print(a)

b=keyrdd.partitionBy(3,partifunc).map(lambda x: x[0]).take(5)
print(b)

c=keyrdd.partitionBy(3,partifunc).map(lambda x: len(set(x))).glom()
print(c.take(6))


d=keyrdd.partitionBy(3,partifunc).map(lambda x:len(set(x))).take(5)
print(d)

final=keyrdd.partitionBy(3,partifunc).map(lambda x: x[0]).glom().map(lambda x:len(set(x)))
for i in final.collect(): print(i)




