from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('df').getOrCreate()

#Create a json column
#you can operate directly on strings of json in spark
jsondf =  spark.range(1).selectExpr(""" 
'{"myjsonkey" : {"myjsonvalue" : [1 , 2 , 3]}}' as jsonstring""")
jsondf.show()

#get_json_object=to inline query a json object
#json_tuple=if object is having only one level of nesting
from pyspark.sql.functions import get_json_object , json_tuple, from_json, to_json,col
jsondf.select(get_json_object(col("jsonstring"), "$.myjsonkey.myjsonvalue[1]").alias("column"),json_tuple(col("jsonstring"),"myjsonkey")).show()