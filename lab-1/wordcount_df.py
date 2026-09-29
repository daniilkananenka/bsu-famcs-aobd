import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder.appName("WordCount DataFrame").getOrCreate()

start = time.time()
df = spark.read.text("/lab/data/text.txt")
words = df.select(explode(split(col("value"), " ")).alias("word"))
counts = words.groupBy("word").count().orderBy(col("count").desc())
counts.show(10)
print("time:", round(time.time() - start, 2), "s")

counts.explain()

spark.stop()
