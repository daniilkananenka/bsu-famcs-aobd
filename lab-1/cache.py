import sys
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Cache").getOrCreate()
sc = spark.sparkContext


def run_actions(words):
    t = []
    start = time.time()
    words.count()
    t.append(time.time() - start)

    start = time.time()
    words.distinct().count()
    t.append(time.time() - start)

    start = time.time()
    words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b).collect()
    t.append(time.time() - start)
    return [round(x, 2) for x in t]


# без кэша
words = sc.textFile("/lab/data/text.txt").flatMap(lambda line: line.split())
t1 = run_actions(words)
print("no cache:", t1, "total", round(sum(t1), 2))

# с кэшем
words = sc.textFile("/lab/data/text.txt").flatMap(lambda line: line.split())
words.setName("words").cache()
t2 = run_actions(words)
print("cache:   ", t2, "total", round(sum(t2), 2))

if len(sys.argv) > 1:
    time.sleep(int(sys.argv[1]))

spark.stop()
