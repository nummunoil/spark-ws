# from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("WordCount")
# sc = SparkContext(conf=conf)

# lines = sc.textFile("file:///opt/bitnami/spark/datasets/book.txt") # = RDD
# # for word in lines.collect():
# #     print(word.encode('ascii', 'ignore'))

# # count by value
# wordCounts = lines.countByValue()
# for word,count in wordCounts.items();
#     print(word.encode('ascii', 'ignore') + str(count))

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///opt/bitnami/spark/datasets/book.txt") #rdd
# for word in lines.collect():
#     print(word.encode('ascii', 'ignore'))

wordCounts=lines.countByValue()
for word,count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))