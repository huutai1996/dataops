from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize Spark session
spark = (SparkSession.builder.appName("AuthorsAges").getOrCreate())
# Create DataFrame
data = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),("TD", 35), ("Brooke", 25)], ["name", "age"])
# Group by name and calculate average age
avg_data = data.groupby("name").agg(avg("age").alias("average_age"))
# Show results
avg_data.show()
