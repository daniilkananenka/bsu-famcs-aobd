import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PageRank").getOrCreate()
sc = spark.sparkContext


def compute_contribs(urls, rank):
    for url in urls:
        yield (url, rank / len(urls))


start = time.time()
lines = sc.textFile("/lab/data/graph.txt")
links = lines.map(lambda line: tuple(line.split())).distinct().groupByKey().cache()
ranks = links.map(lambda x: (x[0], 1.0))

for i in range(10):
    contribs = links.join(ranks).flatMap(lambda x: compute_contribs(x[1][0], x[1][1]))
    ranks = contribs.reduceByKey(lambda a, b: a + b).mapValues(lambda r: 0.15 + 0.85 * r)

top = ranks.takeOrdered(10, key=lambda x: -x[1])
print("time:", round(time.time() - start, 2), "s")
for node, rank in top:
    print(node, round(rank, 4))

spark.stop()
