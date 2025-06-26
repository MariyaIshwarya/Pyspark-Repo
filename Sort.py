from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SortingExample").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

df.orderBy("Age", ascending=False).show()
