from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from sparkmeasure import TaskMetrics, StageMetrics
from pyspark.sql.functions import col, sqrt, length, desc
import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sparky = SparkSession \
    .builder \
    .appName("load_data") \
    .master("yarn") \
    .config("spark.executor.instances", sys.argv[1]) \
    .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23") \
    .getOrCreate() 

sc = sparky.sparkContext

stagemetrics = StageMetrics(sparky)
stagemetrics.begin()

df = sparky.read.format("csv") \
           .option("header", "true") \
           .option("inferSchema", "true") \
           .load("hdfs://okeanos-master:54310"+sys.argv[2])


df.show(10)
sorted_desc_df = df.orderBy(desc("feature_1"))

sorted_desc_df.show()
print(sorted_desc_df.count())

stagemetrics.end()
stagemetrics.print_report()
print(stagemetrics.aggregate_stagemetrics())

patience = 20
while patience > 0:
    try:
        stagemetrics.print_memory_report()
        print("memory report printed")
        patience = -1
    except:
        print("memory report not ready")
        time.sleep(1)
        patience -= 1

if patience == 0:
    print("memory report was never ready :(")
sc.stop()
