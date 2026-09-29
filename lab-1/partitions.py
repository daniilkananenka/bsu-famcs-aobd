import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Partitions").getOrCreate()
sc = spark.sparkContext
path = "/lab/data/text.txt"


def wordcount(rdd, n):
    return rdd.flatMap(lambda line: line.split()) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda a, b: a + b, n)


# шаг 4: разное число партиций
print("partitions | input partitions | time")
for n in [1, 2, 4, 8, 16, 32, 64, 128]:
    rdd = sc.textFile(path, n)
    start = time.time()
    wordcount(rdd, n).count()
    print(n, "|", rdd.getNumPartitions(), "|", round(time.time() - start, 2))

# шаг 5: repartition / coalesce
rdd = sc.textFile(path, 64)
print("\nисходно партиций:", rdd.getNumPartitions())
print("coalesce(4):", rdd.coalesce(4).getNumPartitions())
print("repartition(4):", rdd.repartition(4).getNumPartitions())
print("coalesce(128):", rdd.coalesce(128).getNumPartitions())
print("repartition(128):", rdd.repartition(128).getNumPartitions())

print(rdd.coalesce(4).toDebugString().decode())
print(rdd.repartition(4).toDebugString().decode())

for name, r in [("64 as is", sc.textFile(path, 64)),
                ("coalesce(4)", sc.textFile(path, 64).coalesce(4)),
                ("repartition(4)", sc.textFile(path, 64).repartition(4))]:
    start = time.time()
    wordcount(r, 4).count()
    print(name, round(time.time() - start, 2))

spark.stop()
