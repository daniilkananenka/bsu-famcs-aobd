import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount RDD").getOrCreate()
sc = spark.sparkContext

start = time.time()
lines = sc.textFile("/lab/data/text.txt")
counts = lines.flatMap(lambda line: line.split()) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda a, b: a + b)
top = counts.takeOrdered(10, key=lambda x: -x[1])
print("time:", round(time.time() - start, 2), "s")

for word, count in top:
    print(word, count)
print("distinct words:", counts.count())

spark.stop()
