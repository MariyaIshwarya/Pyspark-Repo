from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SelectFilter").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

df.select("Name").show()
df.filter(df["Age"] > 26).show()
