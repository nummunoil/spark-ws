# from pyspark.sql import SparkSession
# from pyspark.sql import Row

# import collections

# # Create a SparkSession (Note, the config section is only for Windows!)
# spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
# sc = spark.sparkContext

# # people = rdd
# lines = sc.textFile("file:///opt/bitnami/spark/datasets/fakefriends.csv")
# people = lines.map(lambda x: x.split(','))


# # Infer the schema, and register the DataFrame as a table.
# schemaPeople = spark.createDataFrame(people).cache()
# # get DF where age >= 13 and age <= 19

# # to use sql
# schemaPeople.createOrReplaceTempView("people")

# # SQL can be run over DataFrames that have been registered as a table.
# teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# # The results of SQL queries are RDDs and support all the normal RDD operations.
# for teen in teenagers.collect():
#     print(teen)

# spark.stop()
# # Have to stop

from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
sc = spark.sparkContext

def mapper(line):
    fields=line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

# people = rdd
lines = sc.textFile("file:///opt/bitnami/spark/datasets/fakefriends.csv")
people = lines.map(mapper)

# schema = "userId int, name string, age int, friends int"

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache() # DF
schemaPeople.printSchema()
schemaPeople.filter(schemaPeople.age >= 13).filter(schemaPeople.age <= 19).show(schemaPeople.count())

# get DF where age >= 13 and age <= 19

# to use sql
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(teen)

spark.stop()
# Have to stop
