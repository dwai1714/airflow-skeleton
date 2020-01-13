import os

from pyspark.sql import SparkSession
base_path = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession \
    .builder \
    .appName("app_name") \
    .getOrCreate()

df = spark.read \
         .format("csv") \
         .option("header", "true")  \
         .load("file:///" + base_path + "/Book1.csv")
df.show()
