from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

csv=spark.sparkContext.textFile("D:/workspace/pyspark_ex/resouces/Datanet.csv")
acc=spark.sparkContext.accumulator(0)
def acc(datarow):
    Name = datarow["Name"]
    Team = datarow["Team"]
    if Name == "Avery Bradley":
        acc.add(datarow["Age"])
    if Team =="Boston Celtics":
        acc.add(datarow["Age"])
csv.foreach(lambda orders: acc(orders))