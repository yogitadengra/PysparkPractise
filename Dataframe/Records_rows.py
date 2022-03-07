from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yoidata').getOrCreate()
from pyspark.sql import Row
myrow=Row("Hello","yoi","smriti",None,1,False)
print(myrow)


a=myrow[4]
print(a)

Person = Row("name", "age")
p1=Person("James", 40)
p2=Person("Alice", 35)
print(p1.name +","+p2.name)

#excessing value from row
print(p1.age)