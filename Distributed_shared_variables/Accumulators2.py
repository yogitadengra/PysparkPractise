from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
import findspark
findspark.init()


df = spark.read.option("inferSchema", "true").json("D:/workspace/pyspark_ex/resouces/input.json")
counter = spark.sparkContext.accumulator(0)
from pyspark.accumulators import AccumulatorParam
class StringAccumulator(AccumulatorParam):
    def zero(self, initialValue=" "):
        return " "
    def addInPlace(self,s1,s2):
        return s1.strip() + " " +s2.strip()

countervalue = spark.sparkContext.accumulator(" ",StringAccumulator())

def validate(row):
    age=row.Age
    if age < 15:
        counter.add(1)
        countervalue.add(str(age)) #logic

df.foreach(lambda x: validate(x)) #x=for each row
invalidrecords=counter.value # invalid records
print(invalidrecords)

questionablevalue=countervalue.value #questionable value
print(questionablevalue)


