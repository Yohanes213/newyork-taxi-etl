# NYC Yellow Taxi ETL – Week 2 Project

## Overview

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline for the NYC Yellow Taxi January 2024 dataset, as part of a data engineering learning series. The pipeline covers data ingestion, profiling, cleaning, transformation, advanced analytics, and loading into Delta Lake using Databricks and PySpark. The project is designed to follow best practices for scalable, cloud-based data engineering.

For a detailed technical report and all code, see:  
[W2_NYCTaxi_ETL_Report.md](./W2_NYCTaxi_ETL_Report.md)

---

## Prerequisites

- **Azure Account** (recommended): For full cloud storage and Databricks integration.
- **Databricks Workspace**: Community or paid edition. (Most of the project was completed using a paid Databricks subscription; serverless compute was used only after the trial expired near the end.)
- **Python 3.8+** (if running locally for experimentation)
- **PySpark** and **Delta Lake** libraries (Databricks Runtime 16.4 LTS or similar)
- **Dataset**:  
  - [NYC Yellow Taxi January 2024 Parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet)
  - [TLC Taxi Zone Lookup Table (CSV)](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

---

## Setup Instructions

### 1. Download the Dataset

- Download the January 2024 Yellow Taxi data (Parquet) from the [NYC TLC website](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet).
- Download the [Taxi Zone Lookup Table](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv).

### 2. Azure Storage Setup (Recommended)

> **Note:** If you do not have Azure permissions, you can use Databricks' built-in storage or local files as a fallback.

- Create an Azure Blob Storage container or ADLS Gen2 filesystem (e.g., `nyc-taxi-data`).
- Upload the Parquet file to a directory such as:  
  `raw/yellow_taxi/2024/01/yellow_tripdata_2024-01.parquet`
- Note the storage path for use in Databricks.

### 3. Databricks Cluster Setup

- Recommended cluster configuration:
  - **Worker Type:** Standard_D4pds_v6 (16 GB RAM, 4 Cores)
  - **Autoscaling:** Enabled (Min 1, Max 1)
  - **Photon Acceleration:** Enabled
  - **Runtime:** Databricks Runtime 16.4 LTS (includes Spark 3.5.2, Scala 2.13)
- If using serverless or community edition, adjust as needed for your available resources.

### 4. Running the ETL Notebook

- Upload the dataset to your Databricks workspace (e.g., `/FileStore/tables/yellow_tripdata_2024_01.parquet`).
- Open and run the notebook or scripts as described in the [W2_NYCTaxi_ETL_Report.md](./W2_NYCTaxi_ETL_Report.md).
- Update any file paths in the code to match your storage location.

---

## Project Structure

- `W2_NYCTaxi_ETL_Report.md` – Full technical report, code, and analysis for the project.
- `README.md` – This file.
- (Optional) Notebooks or scripts used for the ETL process.

---

## Notes

- If you do not have access to Azure or the Hive metastore, you can still complete all steps using Databricks' built-in storage and Python DataFrame operations, as demonstrated in the report.
- For further details, troubleshooting, and all code, see the [W2_NYCTaxi_ETL_Report.md](./W2_NYCTaxi_ETL_Report.md).

---

## License

This project is for educational purposes.