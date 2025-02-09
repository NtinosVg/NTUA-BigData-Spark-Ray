from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from sparkmeasure import TaskMetrics, StageMetrics
from py4j.java_gateway import java_import
from pyspark.sql.functions import col, sum, count
import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("aggregate_data") \
    .master("yarn") \
    .config("spark.executor.instances", sys.argv[1]) \
    .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23") \
    .getOrCreate()


sc = spark.sparkContext
stagemetrics = StageMetrics(spark)
#java_import(spark._jvm, "ch.cern.sparkmeasure.StageMetrics")
#stage_metrics = spark._jvm.ch.cern.sparkmeasure.StageMetrics(spark._jsparkSession)

#df = spark.read.csv("hdfs://okeanos-master:54310/data/generated_data.csv", header=True, inferSchema=True)
df = spark.read.format("csv") \
           .option("header", "true") \
           .option("inferSchema", "true") \
           .load("hdfs://okeanos-master:54310/"+sys.argv[2])

df.show(10)
stagemetrics.begin()

# GroupBy and Aggregate
df_grouped = (
    df
    .groupBy("categorical_feature_2")
    .agg(
        sum("feature_4").alias("sum_feature_4")
    )
)
df_grouped.show(10)
# Display the grouped and aggregated data
#print("\nGrouped and Aggregated Data Rows:")
#print(df_grouped.count())

stagemetrics.end()
stagemetrics.print_report()

print(stagemetrics.aggregate_stagemetrics())
print("aggregate")
# memory report needs a bit of time to run...
patience = 20
while patience > 0:
    try:
        stagemetrics.print_memory_report()
        print("print_memory_report")
        patience = -1
    except:
        print("memory report not ready")
        time.sleep(1)
        patience -= 1
print("memory report never ready :(")
# Stop Spark
sc.stop()
