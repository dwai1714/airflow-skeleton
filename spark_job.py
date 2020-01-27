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
         .load("http://raw.githubusercontent.com/dwai1714/airflow-skeleton/master/Book1.csv")
df.count()
df.show(100, False)
