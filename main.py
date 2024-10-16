from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("spark project on gcp").getOrCreate()

df = spark.readStream.format("pubsub")