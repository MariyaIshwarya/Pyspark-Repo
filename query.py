from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLQuery").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("people")

spark.sql("""SELECT * FROM people WHERE Age > 25""").show()
