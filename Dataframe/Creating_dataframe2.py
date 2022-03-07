from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
# creating dataframe from data source
#reading csv file
df = spark.read.format("csv").option("header", "true").load("D:/workspace/pyspark_ex/resouces/orders.txt")
df.printSchema()
df.show()

#Creating dataframe with schema
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
#StructType class to define the structure of df and it's a collection or list of StructField objects.
#StructField class  define the columns which includes column name(String), column type (DataType), nullable column (Boolean), metadata (MetaData)


data= [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]

schema = StructType([ \
    StructField("firstname", StringType(), True), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
    ])

df2= spark.createDataFrame(data=data, schema=schema)
df2.printSchema()
df2.show(truncate=False)

