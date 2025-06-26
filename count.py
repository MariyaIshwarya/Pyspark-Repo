from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GroupByExample").getOrCreate()

data = [("Alice", 25), ("Bob", 25), ("Cathy", 30)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

df.groupBy("Age").count().show()
