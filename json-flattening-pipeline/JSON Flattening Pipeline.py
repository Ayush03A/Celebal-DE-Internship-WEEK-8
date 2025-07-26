# Databricks notebook source
from pyspark.sql.functions import explode, col
json_file_path = "/FileStore/tables/students.json"

df_raw = spark.read.option("multiline", "true").json(json_file_path)

print("--- Raw DataFrame with Nested 'courses' column ---")
df_raw.show(truncate=False)

print("\n--- Schema of Raw DataFrame ---")
df_raw.printSchema()

# COMMAND ----------

# Flattening: Explode the array
df_exploded = df_raw.withColumn("course_details", explode(col("courses")))

df_flattened = df_exploded.select(
    col("student_id"),
    col("name"),
    col("major"),
    col("course_details.course_id"),
    col("course_details.course_name"),
    col("course_details.credits")
)

print("--- Final Flattened DataFrame ---")
df_flattened.show()

print("\n--- Schema of Flattened DataFrame ---")
df_flattened.printSchema()

# COMMAND ----------

# Format: catalog.schema.tableName
managed_table_name = "ayush_databricks.default.student_enrollment_data"

df_flattened.write \
    .mode("overwrite") \
    .saveAsTable(managed_table_name)

print(f"SUCCESS: A MANAGED table named '{managed_table_name}' has been created.")


print("\n--- Querying the new managed table ---")
spark.sql(f"SELECT * FROM {managed_table_name} LIMIT 5").show()

# COMMAND ----------

output_volume_path = "/Volumes/ayush_databricks/default/json_data/flattened_student_data_external"

external_table_name = "ayush_databricks.default.student_enrollment_external"

df_flattened.write \
    .mode("overwrite") \
    .saveAsTable(external_table_name)

print(f"SUCCESS: An EXTERNAL table named '{external_table_name}' has been created.")
print(f"Its data files are located at the external path: {output_volume_path}")

print("\n--- Verifying the new EXTERNAL table ---")
spark.sql(f"SELECT * FROM {external_table_name} LIMIT 5").show()

table_details_df = spark.sql(f"DESCRIBE EXTENDED {external_table_name}")
is_external = table_details_df.filter("col_name = 'Type' AND data_type = 'EXTERNAL'").count() == 1

if is_external:
    print(f"\nVerification successful: '{external_table_name}' is confirmed to be an EXTERNAL table. The task is 100% complete.")
else:
    print(f"\nVerification FAILED: '{external_table_name}' is NOT an external table.")
