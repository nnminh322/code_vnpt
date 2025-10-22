# ./app/count_spark.py
import sys, time
from pyspark.sql import SparkSession
N = int(sys.argv[1]) if len(sys.argv)>1 else 1_000_000
spark = SparkSession.builder.appName("CountMillion").getOrCreate()
sc = spark.sparkContext
t0 = time.perf_counter()
rdd = sc.range(1, N+1, numSlices=sc.defaultParallelism)
cnt = rdd.map(lambda _: 1).reduce(lambda a,b: a+b)
print(f"done={cnt}, parts={rdd.getNumPartitions()}, elapsed={time.perf_counter()-t0:.4f}s")
spark.stop()
