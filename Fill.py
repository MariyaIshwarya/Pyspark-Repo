from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MissingData").getOrCreate()

data = [("Alice", 25), ("Bob", None), (None, 28)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

print("Original:")
df.show()

print("Drop nulls:")
df.na.drop().show()

print("Fill nulls:")
df.na.fill("Unknown").show()
