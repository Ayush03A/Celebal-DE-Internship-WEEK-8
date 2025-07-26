# Databricks notebook source
from pyspark.sql.functions import col, sum, avg, desc, count, to_date, to_timestamp, window, lit
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from pyspark.sql import Window

# COMMAND ----------

taxi_data_path = "/Volumes/ayush_databricks/default/taxi_data/yellow_tripdata_2020-01.csv"
zone_lookup_path = "/Volumes/ayush_databricks/default/taxi_data/taxi_zone_lookup.csv"

# Read the taxi data
df_taxi = spark.read.csv(taxi_data_path, header=True, inferSchema=True)

# Read the zone lookup data
df_zone = spark.read.csv(zone_lookup_path, header=True, inferSchema=True)

# Let's look at the first few rows and the schema to understand our data
print("Taxi Data Sample:")
df_taxi.show(5)

print("Taxi Data Schema:")
df_taxi.printSchema()

print("Zone Lookup Data Sample:")
df_zone.show(5)

# COMMAND ----------

numeric_cols = [
    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 
    'tolls_amount', 'improvement_surcharge', 'total_amount', 'passenger_count', 'trip_distance'
]

timestamp_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

# Loop 
for c in numeric_cols:
    df_taxi = df_taxi.withColumn(c, col(c).cast(DoubleType()))

for c in timestamp_cols:
    df_taxi = df_taxi.withColumn(c, col(c).cast(TimestampType()))


print("Schema after Type Casting:")
df_taxi.printSchema()

# COMMAND ----------

# Query 1
# Using withColumn to add a new column named 'Revenue'
df_taxi_with_revenue = df_taxi.withColumn('Revenue', 
    col('fare_amount') + 
    col('extra') + 
    col('mta_tax') + 
    col('improvement_surcharge') + 
    col('tip_amount') + 
    col('tolls_amount') +
    col('total_amount') 
)

# Show the new dataframe with the Revenue column
print("DataFrame with new 'Revenue' column:")
df_taxi_with_revenue.select('total_amount', 'Revenue').show(5)

# COMMAND ----------

# Query 2
# We group by the Pickup Location ID, sum the passenger_count
passengers_by_area = df_taxi.groupBy("PULocationID") \
                            .agg(sum("passenger_count").alias("total_passengers"))

# We join on PULocationID from the taxi data and LocationID from the zone data
passengers_by_area_named = passengers_by_area.join(df_zone, col("PULocationID") == col("LocationID")) \
                                             .select("Borough", "Zone", "total_passengers") \
                                             .orderBy(desc("total_passengers")) 

print("Total passengers by pickup area:")
passengers_by_area_named.show()

# COMMAND ----------

# Query 3
# Group by VendorID and calculate the average for fare_amount and total_amount
avg_fare_by_vendor = df_taxi.groupBy("VendorID") \
                            .agg(
                                avg("fare_amount").alias("avg_fare_amount"),
                                avg("total_amount").alias("avg_total_amount")
                            )

print("Average fare and total amount by Vendor:")
avg_fare_by_vendor.show()

# COMMAND ----------

# CQuery 4
# Defining the window
windowSpec = Window.partitionBy("payment_type").orderBy("tpep_pickup_datetime")

# Create a new column 'moving_count' 
payment_moving_count = df_taxi.withColumn("moving_count", count("*").over(windowSpec))

# Display the results. I select a few columns to make it clear.
print("Moving count of payments by mode (showing a few for payment_type=1):")
payment_moving_count.select("payment_type", "tpep_pickup_datetime", "moving_count") \
                    .where(col("payment_type") == 1).show()

# COMMAND ----------

# Query 5
# First, filter the data for a single day
single_day_df = df_taxi_with_revenue.where(to_date(col("tpep_pickup_datetime")) == '2020-01-15')

# Now, group by vendor
top_2_vendors_oneday = single_day_df.groupBy("VendorID") \
    .agg(
        sum("Revenue").alias("total_revenue"),
        sum("passenger_count").alias("total_passengers"),
        sum("trip_distance").alias("total_distance_miles")
    ) \
    .orderBy(desc("total_revenue")) \
    .limit(2)

print("Top 2 highest earning vendors on 2020-01-15:")
top_2_vendors_oneday.show()

# COMMAND ----------

# Query 6
# Group by both Pickup and Dropoff Location IDs
route_passengers = df_taxi.groupBy("PULocationID", "DOLocationID") \
                          .agg(sum("passenger_count").alias("total_passengers"))

# We create aliases for the zone table 
df_zone_pickup = df_zone.withColumnRenamed("Zone", "Pickup_Zone").withColumnRenamed("Borough", "Pickup_Borough")
df_zone_dropoff = df_zone.withColumnRenamed("Zone", "Dropoff_Zone").withColumnRenamed("Borough", "Dropoff_Borough")

# Join to get pickup names, then join to get dropoff names
popular_routes = route_passengers.join(df_zone_pickup, col("PULocationID") == df_zone_pickup["LocationID"]) \
                                 .join(df_zone_dropoff, col("DOLocationID") == df_zone_dropoff["LocationID"]) \
                                 .select("Pickup_Borough", "Pickup_Zone", "Dropoff_Borough", "Dropoff_Zone", "total_passengers") \
                                 .orderBy(desc("total_passengers"))

print("Most popular routes by passenger count:")
popular_routes.show(10)

# COMMAND ----------

from pyspark.sql.functions import col, max as F_max, sum as F_sum
from datetime import timedelta

# Query 7
# First, find the latest dropoff time in the entire dataset
latest_timestamp = df_taxi.select(F_max("tpep_pickup_datetime")).first()[0]
print(f"The latest pickup timestamp in the dataset is: {latest_timestamp}")

# Calculate the start of our 10-second window
ten_seconds_before = latest_timestamp - timedelta(seconds=10)

# Filter the DataFrame for records within this 10-second window
last_10_seconds_df = df_taxi.where(col("tpep_pickup_datetime") >= ten_seconds_before)

# Now, perform the aggregation on this small, time-filtered DataFrame
top_locations_last_10s = last_10_seconds_df.groupBy("PULocationID") \
    .agg(F_sum("passenger_count").alias("total_passengers")) \
    .join(df_zone, col("PULocationID") == col("LocationID")) \
    .select("Borough", "Zone", "total_passengers") \
    .orderBy(desc("total_passengers"))

display(top_locations_last_10s)

# COMMAND ----------

output_parquet_path = "/Volumes/ayush_databricks/default/taxi_data/processed_taxi_data.parquet"


# We write the processed DataFrame to the specified Parquet path in your Volume.
df_taxi_with_revenue.write.mode("overwrite").parquet(output_parquet_path)

print(f"Successfully wrote the DataFrame to your Volume at: {output_parquet_path}")


# COMMAND ----------

# Import all the functions we will need for the upcoming queries
from pyspark.sql.functions import col, sum, avg, desc, count, to_date, to_timestamp, window, lit, max
from pyspark.sql import Window
from datetime import timedelta

# Load our processed table into a DataFrame
# This is the table that already includes the 'Revenue' column
processed_taxi_df = spark.table("ayush_databricks.default.processed_nyc_taxi_data")

# Load our zone lookup table into a DataFrame for easy joins
zone_df = spark.table("ayush_databricks.default.zone_lookup")

print("DataFrames are loaded and ready for queries.")

# COMMAND ----------

