from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("Big Data Analysis with PySpark") \
    .getOrCreate()

# Step 2: Load large CSV file
df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

# Step 3: Show the structure of the dataset
df.printSchema()

# Step 4: Basic analysis - total sales per category
sales_by_category = df.groupBy("Category") \
    .agg(_sum("Sales").alias("Total_Sales")) \
    .orderBy("Total_Sales", ascending=False)

sales_by_category.show()

# Step 5: Average sales per product
avg_sales = df.groupBy("Product") \
    .agg(avg("Sales").alias("Average_Sales")) \
    .orderBy("Average_Sales", ascending=False)

avg_sales.show()

# Step 6: Filter data (e.g., for a specific country)
country_data = df.filter(col("Country") == "India")
country_data.show(5)

# Stop Spark session
spark.stop()
