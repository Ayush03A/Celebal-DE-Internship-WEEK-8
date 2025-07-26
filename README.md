# Week 8 Assignment: Big Data Processing with Databricks & PySpark 🚀

## 📋 Project Overview

This project demonstrates core data engineering and big data analytics skills using **Databricks** and **Apache Spark (PySpark)**. The focus is on building robust data pipelines for ingesting, transforming, and analyzing both structured and semi-structured data within a modern, governed **Unity Catalog** environment.

> 🔗 **Thanks to [CSI (Celebal Summer Internship)](https://www.celebaltech.com/)**  
> This assignment provided deep, hands-on experience with Spark's distributed computing capabilities, data modeling in a data lakehouse, and the real-world challenges of debugging in a secure, cloud-native environment.

---

## 🛠️ Technologies Used

- **Platform:** Databricks
- **Compute Engine:** Apache Spark (via PySpark)
- **Data Governance:** Unity Catalog (Volumes & Tables)
- **Data Formats:** CSV, JSON, Parquet
- **Notebook Language:** Python
- **Key Concepts:** DataFrames, ETL, Window Functions, Data Engineering Patterns

---

## 📂 Project Tasks & Implementation

### 1️⃣ NYC Taxi Data Analysis 🚕

**Objective:** Ingest, clean, analyze, and persist a large, real-world CSV dataset to answer several business intelligence questions.

#### Steps & Implementation:
- **Environment:** Configured a Databricks cluster and notebook.
- **Ingestion:** Uploaded the NYC Taxi and Zone Lookup datasets into a **Unity Catalog Volume**.
- **Transformation:**
  - Loaded data into PySpark DataFrames.
  - Performed crucial **data type casting** for numeric and timestamp columns.
  - Added a new `Revenue` column using `withColumn()`.
- **Analysis:** Executed 7 complex queries using the DataFrame API to derive insights, including aggregations, joins, and window functions.
- **Persistence:** Saved the final enriched DataFrame as a queryable table in Unity Catalog.

📸 **Evidence:**

- ✅ Cluster is up and running in the Databricks environment.

  ![Cluster Running](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/be92f5f87adaa6c4d9ef9fe086aa3da8540f1167/nyc-taxi-analysis/Screenshots/Cluster%20is%20up%20and%20running%20in%20the%20Databricks%20environment.png)
  
- ✅ Raw CSV data successfully uploaded to a Unity Catalog Volume.
  
  ![Data in Volume](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/be92f5f87adaa6c4d9ef9fe086aa3da8540f1167/nyc-taxi-analysis/Screenshots/Uploading%20yellow_tripdata_2020-01.Csv....png)
  
- ✅ Output of a complex query (e.g., Most Popular Routes) showing successful data joining and aggregation.

  ![Query Result](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/be92f5f87adaa6c4d9ef9fe086aa3da8540f1167/nyc-taxi-analysis/Screenshots/%20Query%206%20(Most%20Popular%20Route).png)
  
- ✅ Final, enriched table visible and queryable in the Catalog Explorer.

  ![processed_taxi_data](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/be92f5f87adaa6c4d9ef9fe086aa3da8540f1167/nyc-taxi-analysis/Screenshots/processed_taxi_data.png)


### 📊 Data Visualization & Dashboard

To better communicate the findings from the analysis, several key visualizations were created to form a dashboard. These charts highlight important trends and patterns in the data at a glance, turning raw query results into actionable insights.

| Visualization & Insight                                      | Screenshot                                                                            |
| :----------------------------------------------------------- | :------------------------------------------------------------------------------------ |
| **Daily Fare vs. Distance Trends** <br> *Shows a clear positive correlation between trip distance and fare amount, with some variation by day of the week.* | ![Fare vs Distance](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/be92f5f87adaa6c4d9ef9fe086aa3da8540f1167/nyc-taxi-analysis/Screenshots/Daily%20Fare%20Trends%20by%20Day%20of%20Week.png)      |
| **Pickup Hour Distribution** <br> *Visualizes the number of taxi rides starting at each hour, revealing daily and weekly demand cycles and a noticeable dip around the end of January.* | ![Pickup Distribution](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/be92f5f87adaa6c4d9ef9fe086aa3da8540f1167/nyc-taxi-analysis/Screenshots/Pickup%20Hour%20Distribution.png)  |
| **Dropoff Hour Distribution** <br> *Similar to pickups, this chart shows when trips typically end, confirming the overall ride patterns and demand flow throughout the day.* | ![Dropoff Distribution](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/be92f5f87adaa6c4d9ef9fe086aa3da8540f1167/nyc-taxi-analysis/Screenshots/Dropoff%20Hour%20Distribution.png) |
| **Route Trip Attribution** <br> *A tabular view identifying the most frequent routes (by ID), which is essential for understanding high-traffic corridors and operational planning.* | ![Top Routes](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/be92f5f87adaa6c4d9ef9fe086aa3da8540f1167/nyc-taxi-analysis/Screenshots/Route%20Revenue%20Attribution.png)         |


---

### 2️⃣ JSON Flattening Data Engineering Pipeline 🔩

**Objective:** Process a nested JSON dataset, transform it into a flat, structured format, and persist it as a proper **External Table** in Unity Catalog.

#### Steps & Implementation:
- **Ingestion:** Loaded a sample `students.json` file with nested arrays into DBFS.
- **Flattening:**
  - Used the `explode` function to create new rows from the nested `courses` array.
  - Promoted the fields from the resulting `struct` to top-level columns, creating a clean, flat schema.
- **Persistence:** Navigated several Unity Catalog challenges to successfully create a true **External Table**. This required a specific, robust pattern:
  1.  Writing the flattened DataFrame to a **Unity Catalog Volume** as Parquet files.
  2.  Using the programmatic **`spark.catalog.createTable` API** to create the table metadata pointing to the Volume path.

📸 **Evidence:**
- ✅ Raw DataFrame showing the initial nested structure of the `courses` column.

  ![Raw Data](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/828d7b7b816b96d109ac0c3985d190c375869a4f/json-flattening-pipeline/Screenshots/Raw%20Data%20Loaded.png)
  
- ✅ Final DataFrame after the `explode` and `select` transformations, showing a perfectly flat schema.

  ![Flattened Data](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/828d7b7b816b96d109ac0c3985d190c375869a4f/json-flattening-pipeline/Screenshots/Flattened%20Data.png)
  
- ✅ Final success message and verification output confirming that a true **EXTERNAL** table was created.

  ![Final Table](https://github.com/Ayush03A/Celebal-DE-Internship-WEEK-8/blob/828d7b7b816b96d109ac0c3985d190c375869a4f/json-flattening-pipeline/Screenshots/Final%20Table%20Created.png)

---

## ✅ Summary Table

| Task                              | Status   |
| --------------------------------- | -------- |
| NYC Taxi Data Ingestion & Cleaning| ✅ Done  |
| 7 Analytical Queries Executed     | ✅ Done  |
| Nested JSON Data Flattening       | ✅ Done  |
| Creation of a True External Table | ✅ Done  |

---

## ❗ Challenges Faced

- 💻 I am running code from my **MacBook via Databricks Connect**, which introduced complexities not present when running directly in a web notebook.
- 🔐 The primary challenge was the persistent **`Missing cloud file system scheme`** error when trying to create an external table in Unity Catalog.
  - 👉 **Initial attempts** using raw SQL (`CREATE TABLE ... LOCATION ...`) and the `.saveAsTable()` with `.option("path", ...)` failed due to the remote client's inability to resolve the `/Volumes/` path.
  - 🔑 **The Fix:** Discovered that this was an environment issue. The solution was a two-part fix:
    1.  Changing the **Cluster Access Mode** from `Shared` to **`Single User`**.
    2.  Using the correct programmatic API, **`spark.catalog.createTable()`**, which is designed to work correctly from remote clients like Databricks Connect.

---

## 🧠 Key Learnings

- **PySpark for Large-Scale ETL:** Gained practical experience in using Spark's DataFrame API for efficient data manipulation and analysis.
- **Unity Catalog Best Practices:** Learned the critical difference between **Managed** and **External** tables and the correct patterns for creating each, especially in a modern, secure Unity Catalog environment.
- **Advanced Debugging:** This project provided invaluable experience in diagnosing and solving non-obvious environment and configuration issues. Understanding the role of **Cluster Access Mode** and the behavior of **Databricks Connect** was a major takeaway.
- **Data Engineering Resilience:** The final working pattern (writing data to a Volume, then using the Catalog API to create the table) is a robust and reliable method for building data pipelines programmatically.

---

## 🙏 Acknowledgements

A huge thank you to the entire team at Celebal for this incredible learning opportunity and to my mentors for their guidance.

👨‍🏫 **Jash Tewani, Anurag Yadav & Ajit Kumar Singh** – Technical Mentor  
🙌 **Prerna Kamat** – HR, Celebal CSI  
🙌 **Priyanshi Jain** – HR, Celebal CSI  
🏢 **Celebal Technologies** – For providing a platform to work on real-world data engineering challenges.
