from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///opt/bitnami/spark/datasets/fakefriends-header.csv")
# read file with header to schema & data

print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show(10)

print("Group by age")
people.groupBy("age").count().orderBy("age").show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

# desc_path="file:///opt/bitnami/spark/datasets/output/people"
# people.write.mode('overwrite').save(desc_path)

desc_path="file:///opt/bitnami/spark/datasets/output/people_json"
people.write.format('json').mode('overwrite').save(desc_path)

spark.stop()
