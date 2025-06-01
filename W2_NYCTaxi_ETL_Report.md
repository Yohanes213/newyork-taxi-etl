# New York Taxi ETL

## 1. Introduction

In this report, I document the design and implementation of an ETL (Extract, Transform, Load) pipeline for the NYC Yellow Taxi trip data (January 2024). The goal is to ingest, clean, transform, and analyze the data using PySpark and Delta Lake within Databricks, following best practices for data engineering and analytics.

## 2. Business Objective / Background Context



## 3. Environment & Tools



## 4. ETL Process and Tasks

### 4.1 Task 1: Data Ingestion, Environment Preparation, and Initial Exploration 


**1.5: Initial Data Profiling and Understanding**

I started  with simple data understanding methods like cheking the schema, the row counts and so on. This are some of the findings from my analysis:

- **Schema & Dictionary Alignment**

    The dataset's column names, data types, and value domains exactly match the official TLC data dictionary. This confirms our ingestion step preserved fidelity to source definitions.
    <br>
    <br>

- **Dataset Size**

    We have 2,964,624 trip records for January 2024—ample volume for robust downstream analysis.

- **Descriptive Statistics**

    Running df.describe().show() surfaced an unexpected outlier:

    1.  **Missing Values**: Columns such as `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, and `Airport_fee` have fewer rows (2,824,462) than other columns (2,964,624), indicating approximately 140,162 missing values (4.73% of total rows), consistent with the null elements analysis.
    2. **Data Variability**:
   - `trip_distance` exhibits extreme variability, with a standard deviation of 225.46 and a maximum value of 312,722.3, suggesting potential outliers or data entry errors.
   - `fare_amount` and `total_amount` show large ranges (-899.0 to 5,000.0 and -900.0 to 5,000.0, respectively), with negative values indicating possible refunds or data issues.
   - `RatecodeID` has a high standard deviation (9.82) and a maximum of 99, which may reflect non-standard or erroneous rate codes.
    3. **Categorical Columns**:
   - `VendorID`, `payment_type`, and `store_and_fwd_flag` have low variability, with 3, 5, and 2 distinct values, respectively, indicating limited categories.
   - `PULocationID` and `DOLocationID` range from 1 to 265, reflecting a large but finite set of pickup and drop-off locations.
    4. **Monetary Columns**: Columns like `fare_amount`, `tip_amount`, `tolls_amount`, and `total_amount` show negative minimum values, which may indicate data quality issues or specific business cases (e.g., refunds or adjustments).
   - `tip_amount` (mean 3.34, max 428.0) suggest right-skewed distributions, with most values being low but some extreme outliers.


- **Anomalies Detected: Negative Monetary Values**

   I checked for negative values in fields that should only be additive charges. The results:

    | Column                | # Negative Rows | % of Total Rows |
    |-----------------------|-----------------|-----------------|
    | fare_amount           | 37,448          | 1.26%           |
    | extra                 | 17,548          | 0.59%           |
    | mta_tax               | 34,434          | 1.16%           |
    | tip_amount            | 102             | 0.00%           |
    | tolls_amount          | 2,035           | 0.07%           |
    | improvement_surcharge | 35,502          | 1.20%           |
    | total_amount          | 35,504          | 1.20%           |
    | congestion_surcharge  | 28,825          | 0.97%           |
    | airport_fee           | 4,921           | 0.17%           |

    These negative values are not expected in normal trip records and likely indicate post-trip reversals, disputes, or data errors.


-  **Null Elements Analysis**

    An analysis was conducted to identify null values across the dataset's columns. The results are summarized in the table below:

    | Column                | # Null Rows | % of Total Rows |
    |-----------------------|-------------|-----------------|
    | passenger_count       | 140,162     | 4.73%           |
    | RatecodeID            | 140,162     | 4.73%           |
    | store_and_fwd_flag    | 140,162     | 4.73%           |
    | congestion_surcharge  | 140,162     | 4.73%           |
    | Airport_fee           | 140,162     | 4.73%           |

    Notably, all listed columns exhibit the same number of null values (140,162), representing 4.73% of the total rows. This observation suggests a potential correlation where a null value in one column implies null values in the others.

    To verify this, a filter was applied to isolate rows where `store_and_fwd_flag` is null:

    ```python
    null_store = df.filter(col("store_and_fwd_flag").isNull())
    ```

    Subsequently, the null counts for the other columns were checked within this filtered dataset:



    ```python
    null_store.select([
    count(when(col(c).isNull(), 1)).alias(c)
    for c in ["passenger_count", "RatecodeID", "congestion_surcharge", "Airport_fee"]
    ]).show()
    ```
    The results confirmed that each of the other columns also had 140,162 null values in the filtered dataset, indicating that null values occur simultaneously across these columns.


- **Distinct Elements Analysis**

    An analysis was performed to determine the number of unique values (distinct elements) for each column in the dataset. The results are summarized in the table below:

    | Column                | # Distinct Elements |
    |-----------------------|---------------------|
    | VendorID              | 3                   |
    | tpep_pickup_datetime  | 1,575,706           |
    | tpep_dropoff_datetime | 1,574,780           |
    | passenger_count       | 10                  |
    | trip_distance         | 4,489               |
    | RatecodeID            | 7                   |
    | store_and_fwd_flag    | 2                   |
    | PULocationID          | 260                 |
    | DOLocationID          | 261                 |
    | payment_type          | 5                   |
    | fare_amount           | 8,970               |
    | extra                 | 48                  |
    | mta_tax               | 8                   |
    | tip_amount            | 4,192               |
    | tolls_amount          | 1,127               |
    | improvement_surcharge | 5                   |
    | total_amount          | 19,241              |
    | congestion_surcharge  | 6                   |
    | Airport_fee           | 3                   |


    This analysis highlights the variability of values within each column. Notably, columns such as `tpep_pickup_datetime` and `tpep_dropoff_datetime` have a high number of distinct elements (1,575,706 and 1,574,780, respectively), indicating a wide range of unique timestamps. In contrast, columns like `VendorID`, `store_and_fwd_flag`, and `Airport_fee` have very few distinct values (3, 2, and 3, respectively), suggesting limited variability. The high number of unique values in columns like `total_amount` (19,241) and `fare_amount` (8,970) reflects the diverse range of monetary values in the dataset.

#### 4.2 Data Cleaning Strategy

Based on the initial profiling and the official TLC data dictionary, I identified several data quality issues and developed a targeted cleaning strategy. Below, I detail the main issues, the rationale for each cleaning step, and the specific actions taken.

---

**1. Trip Distance Outliers and Inconsistencies**

- **Issue:**  
  The `trip_distance` column exhibited extreme outliers (e.g., values over 300,000 miles) and a significant number of zero values. According to the data dictionary, this field should represent the actual miles traveled, and such values are not plausible for NYC taxi trips.
- **Action:**  
  - **Calculated the 99.9th percentile** for `trip_distance` to understand its distribution and identify a reasonable upper bound for realistic trips. The result was 29.51 miles.
  ```python
  raw_taxi_df.approxQuantile("trip_distance", [0.999], 0.0)[0]
  ```
  - **Created a new column, `time_take_min`**, representing trip duration in minutes, to cross-validate trip distance with time, which is essential for calculating speed.
  ```python
  raw_taxi_df = raw_taxi_df.withColumn("time_take_min", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0)
  ```
  - **Filtered out trips with implausible average speeds** (over 50 mph). Given the nature of city taxi data, it is highly unlikely for a vehicle to maintain such a high speed for an extended duration.
  ```python
  df_valid_speed = raw_taxi_df.filter(
      (col("trip_distance") / (col("time_take_min")/60)) <= 50
  )
  ```
  - After validating speed, **filtered out remaining rows with `trip_distance` greater than 50 miles**, as these are still considered extreme outliers even after speed-based filtering.
- **Rationale:**  
  These steps ensure that only realistic taxi trips are retained, significantly improving the reliability of downstream analyses by removing physically impossible or highly erroneous records.

---

**2. Negative and Implausible Fare and Charge Values**

- **Issue:**  
  Several monetary columns (`fare_amount`, `total_amount`, `tip_amount`, etc.) contained negative values, which are not expected under normal circumstances according to the data dictionary. While some negative values may represent refunds or adjustments, many are likely data entry errors.
- **Action:**  
  - **Identified and filtered out a specific set of negative fare records**: those with negative fare, standard payment types (0, 1, 2: Flex Fare, Credit Card, Cash), and very short trip distances (less than 1 mile). These were deemed inconsistent with typical taxi operations.
  ```python
  df_valid_fare_first = df_clean.filter(
     ~ (
      (col("fare_amount") < 0) &
      (col("payment_type").isin(0, 1, 2)) &
      (col("trip_distance") < 1)
      )
  )
  ```
  - For the **remaining negative values in monetary columns**, if the `payment_type` indicated a standard transaction (0, 1, 2), I **converted them to positive** by multiplying by -1. This assumes data entry errors where a negative sign was mistakenly included for legitimate charges. This was applied to: `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, and `airport_fee`.
  ```python
  df_valid_fare_second = df_valid_fare_first.withColumn(
      "fare_amount",
      when(
          (col("fare_amount") < 0) &
          (col("payment_type").isin(0, 1, 2)),
          -col("fare_amount")
  ).otherwise(col("fare_amount"))
              
  ).withColumn(
      "extra",
      when(
          (col("extra") < 0) &
          (col("payment_type").isin(0, 1, 2)),
          -col("extra")
  ).otherwise(col("extra"))
              
  ).withColumn(
      "mta_tax",
      when(
          (col("mta_tax") < 0) &
          (col("payment_type").isin(0, 1, 2)),
           -col("mta_tax")
  ).otherwise(col("mta_tax"))

  ).withColumn(
      "tip_amount",
      when(
          (col("tip_amount") < 0) & 
          (col("payment_type").isin(0, 1, 2)),
          -col("tip_amount")
  ).otherwise(col("tip_amount"))

  ).withColumn(
      "tolls_amount",
      when(
          (col("tolls_amount") < 0) &
          (col("payment_type").isin(0, 1, 2)), 
          -col("tolls_amount")
  ).otherwise(col("tolls_amount"))

  ).withColumn(
      "improvement_surcharge",
      when(
          (col("improvement_surcharge") < 0) &
          (col("payment_type").isin(0, 1, 2)), 
          -col("improvement_surcharge")
  ).otherwise(col("improvement_surcharge"))

  ).withColumn(
      "total_amount",
      when(
          (col("total_amount") < 0) &
          (col("payment_type").isin(0, 1, 2)), 
          -col("total_amount")
  ).otherwise(col("total_amount"))
      
  ).withColumn(
      "congestion_surcharge",
      when(
          (col("congestion_surcharge") < 0) &
          (col("payment_type").isin(0, 1, 2)), 
          -col("congestion_surcharge")
  ).otherwise(col("congestion_surcharge"))

  ).withColumn(
      "airport_fee",
      when(
          (col("airport_fee") < 0) &
          (col("payment_type").isin(0, 1, 2)), 
          -col("airport_fee")
  ).otherwise(col("airport_fee"))
  )
  ```
- **Rationale:**  
  This approach aims to preserve legitimate refund/dispute records (which might have different payment types) while correcting likely data entry mistakes for standard transactions, aligning with the data dictionary's intent for these monetary fields.

---

**3. Trip Duration and Timestamp Corrections**

- **Issue:**  
  The calculated `time_take_min` column contained negative values, indicating that `tpep_pickup_datetime` occurred *after* `tpep_dropoff_datetime`. This suggests an interchange error in the source data.
- **Action:**  
  - Identified rows where `time_take_min` was negative.
  - For these specific rows, the values in `tpep_pickup_datetime` and `tpep_dropoff_datetime` were swapped to correct the chronological order.
- **Rationale:**  
  Ensures all trips have a positive and logically correct duration, which is a fundamental requirement for accurate trip records and subsequent time-based analysis.

---

**4. Zero Trip Distance: Business Logic for Retention or Removal**

- **Issue:**  
  Trips with zero `trip_distance` can be ambiguous. While some may indicate valid scenarios like a flag drop or very short crawl, others could represent canceled trips or data errors.
- **Action:**  
  - Developed a comprehensive set of business rules to classify zero-distance trips as "keep" or "drop" by creating a `keep_or_drop` column, considering `time_take_min`, `fare_amount`, and `payment_type`:
  ```python
  df_clean = df_valid_distance.withColumn(
      "keep_or_drop",
      when(
          # 1) Flag-drop rides -> KEEP: Short duration, minimum fare, standard payment types.
          (col("trip_distance") == 0) &
          (col("time_take_min") < 2) &
          (col("fare_amount") >= 4.50) &
          (col("payment_type").isin(1, 2)),
          lit("keep")
      ).when(
          # 2) Rounding-artifact (short crawl) -> KEEP: Slightly longer duration for a "zero" distance trip, minimum fare, standard payment types.
          (col("trip_distance") == 0) &
          (col("time_take_min") >= 2) &
          (col("time_take_min") < 10) &
          (col("fare_amount") >= 4.50) &
          (col("payment_type").isin(1, 2)),
          lit("keep")
      ).when(
          # 3) Canceled / No-Charge / Voided -> DROP: Zero distance with payment types indicating no charge or void.
          (col("trip_distance") == 0) &
          (col("payment_type").isin(3, 4, 6)),
          lit("drop")
      ).when(
          # 4) Meter-glitch (>=10 min on meter, fare = 0) -> DROP: Long duration but no fare, suggesting a data error.
          (col("trip_distance") == 0) &
          (col("time_take_min") >= 10) &
          (col("fare_amount") == 0),
          lit("drop")
      ).when(
          # 5) High-fare, short-time anomaly -> DROP: High fare for a very short duration and zero distance.
          (col("trip_distance") == 0) &
          (col("time_take_min") < 5) &
          (col("fare_amount") > 20),
          lit("drop")
      ).otherwise(
          # 6) Everything else (including trip_distance > 0) -> KEEP
          lit("keep")
      )
  )
  ```
  - After applying this logic, the DataFrame was filtered to `keep` only the valid zero-distance trips and all trips with `trip_distance` greater than zero.
- **Rationale:**  
  This nuanced approach retains legitimate short trips (like flag drops) while accurately removing likely errors or non-service records, aligning with the operational realities of a taxi service.

---

**5. Fare Validation Against TLC Rules**

- **Issue:**  
  Despite cleaning negative values, some `fare_amount` entries might still be inconsistent with the official NYC Taxi & Limousine Commission (TLC) fare structure.
- **Action:**  
  - Calculated `distance_fare` and `time_fare` based on the documented rates.
  - Determined the `meter_increment` as the greater of `distance_fare` or `time_fare`, as per TLC rules.
  - Computed an `expected_meter_fare` based on `RatecodeID` and the specific fare rules for standard rates, JFK, Newark, and Nassau/Westchester.
  - Filtered out trips where the `fare_amount` deviated significantly (more than +$10 or less than -$1) from the `expected_meter_fare`.
  ```python
  df = df_valid_fare_second

  df = df.withColumn("distance_fare", col("trip_distance") * lit(3.50))
  df = df.withColumn("time_fare", col("time_take_min") * lit(0.70))

  # The meter always chooses the larger of those two, _per trip_:
  df = df.withColumn("meter_increment", greatest(col("distance_fare"), col("time_fare")))

  # Compute expected_fare based on RatecodeID
  df = df.withColumn(
      "expected_meter_fare",
      when(
          col("RatecodeID") == 1,        # Standard metered rate
          lit(3.00) + col("meter_increment")
      ).when(
          col("RatecodeID") == 2,        # JFK flat fare
          lit(70.00)                     # The meter simply reads $70.00
      ).when(
          col("RatecodeID") == 3,        # Newark trips use standard meter + $20 surcharge
          lit(3.00) + col("meter_increment") + lit(20.00)
      ).when(
          col("RatecodeID") == 4,        # Nassau/Westchester: 1.5x standard meter
          lit(3.00) + (col("meter_increment") * lit(1.5))
      ).otherwise(lit(None))
  )

  df_valid_fare = df.filter(
      ~ (col("fare_amount") > col("expected_meter_fare") + lit(10.00))
      | (col("fare_amount") < col("expected_meter_fare") - lit(1.00))
  )
  ```
- **Rationale:**  
  This validation step enforces compliance with official fare regulations, significantly increasing the accuracy and trustworthiness of the `fare_amount` column.

---

**6. Null Values and Simultaneous Missingness**

- **Issue:**  
  Columns such as `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, and `airport_fee` had identical null counts, always missing together.
- **Action:**  
  - Confirmed the correlation of these nulls through targeted filtering (as shown in the initial profiling section).
  - After applying the various cleaning and filtering steps mentioned above (e.g., speed, fare validation), a re-check confirmed that all null values in these correlated columns were effectively handled or removed. Therefore, no explicit imputation strategy was required for these columns post-cleaning.
- **Rationale:**  
  Ensures that only complete and reliable records are retained for analysis, as the primary cleaning steps implicitly resolved these null issues.

---

**7. Data Type Consistency**

- **Issue:**  
  Some columns were not in their correct data types as per the data dictionary, which can hinder accurate calculations and analysis (e.g., `passenger_count` was loaded as a string).
- **Action:**  
  - Cast `passenger_count`, `RatecodeID`, and `payment_type` to `IntegerType`.
  - Cast `tpep_pickup_datetime` and `tpep_dropoff_datetime` to `TimestampType`.
- **Rationale:**  
  Guarantees schema consistency and enables correct analytical operations and aggregations.

---

**8. Invalid Passenger Counts**

- **Issue:**  
  The `passenger_count` column contained entries with `0` passengers, which are not valid for a completed taxi trip.
- **Action:**  
  - Filtered out rows where `passenger_count` was equal to `0`.
- **Rationale:**  
  Ensures that only records representing actual trips with passengers are included in the dataset, improving the validity of passenger-related analyses.

---

**9. Feature Engineering**

- **Action:**  
  In addition to `time_take_min`, several new columns were created to enrich the dataset for more comprehensive analysis:
  - **`average_speed`**: Calculated from `trip_distance` and `time_take_min`, providing insight into trip efficiency.
  - **`pickup_day_of_week`** and **`pickup_hour_of_day`**: Extracted from `tpep_pickup_datetime` to analyze temporal patterns.
  ```python
  df_date = df_speed.withColumn(
  "pickup_hour_of_day",
  hour(col("tpep_pickup_datetime"))
  ).withColumn(
      "pickup_day_of_week",
      dayofweek(col("tpep_pickup_datetime")
  )
  )
  ```
  - **`time_of_day_slot`**: Categorized `pickup_hour_of_day` into distinct time slots (e.g., "Night", "Morning", "Afternoon", "Evening", "LateNight") for broader temporal analysis.
  ```python
  df_date = df_date.withColumn(
  "time_of_day_slot",
  when(col("pickup_hour_of_day").between(0, 5),    lit("Night"))
  .when(col("pickup_hour_of_day").between(6, 11),   lit("Morning"))
  .when(col("pickup_hour_of_day").between(12, 16),  lit("Afternoon"))
  .when(col("pickup_hour_of_day").between(17, 20),  lit("Evening"))
  .otherwise(lit("LateNight"))
  )
  ```
- **Rationale:**  
  These derived features provide valuable dimensions for analytical queries, enabling deeper insights into taxi trip patterns and passenger behavior.

---

**Summary Table of Cleaning Actions**

| Issue                           | Action Taken                                                                 | Rationale/Reference                |
|----------------------------------|------------------------------------------------------------------------------|------------------------------------|
| Trip distance outliers           | Filtered by 99.9th percentile, speed, and max threshold                      | Data dictionary, NYC geography     |
| Negative monetary values         | Filtered specific cases; converted remaining based on payment type           | Data dictionary, business logic    |
| Negative trip durations          | Swapped pickup/dropoff times                                                 | Data integrity                     |
| Zero trip distance               | Business rules to keep/drop based on fare, time, payment type                | TLC business rules                 |
| Nulls in correlated columns      | Resolved implicitly by other cleaning steps                                  | Data completeness                  |
| Data type mismatches             | Cast columns to correct types                                                | Data dictionary                    |
| Invalid passenger counts         | Removed rows with zero passengers                                            | Data dictionary                    |
| Implausible fares                | Validated against TLC fare structure                                         | TLC fare rules                     |
| Feature engineering              | Added speed, time slots, day of week, etc.                                   | Analytical value                   |

---

**Conclusion:**  
This comprehensive cleaning strategy, grounded in the data dictionary, official TLC rules, and business logic, ensures that the dataset is accurate, consistent, and well-prepared for robust analysis and subsequent loading into Delta Lake.


