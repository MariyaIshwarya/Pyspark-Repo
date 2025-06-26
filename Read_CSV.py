from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

df = spark.read.csv("sample.csv", header=True, inferSchema=True)
df.show()
