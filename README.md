# NYC Taxi Data Engineering Project

## Overview
This project is designed to extract, transform, and load (ETL) NYC Taxi data for the years 2019 and 2020. The goal is to provide analytical insights using a robust data pipeline built with Apache Airflow, Docker, and PostgreSQL. The data is further refined using DBT, culminating in a comprehensive dashboard developed with Looker.

## Purpose and Motivation
The main reason for this project is to provide an automated data pipeline that addresses real-world data engineering challenges. By leveraging publicly available data, we can create actionable insights, focusing on key performance indicators like total trips, revenue, and customer behavior.

## Pipeline Architecture
![Pipeline Diagram](path/to/pipeline-diagram.png)

The architecture includes:
- **Data Extraction**: Using Airflow to automate the downloading and ingestion of data from the NYC Taxi dataset.
- **Data Transformation**: Applying DBT to cleanse and transform the data into a usable format.
- **Data Loading**: Storing the processed data in a PostgreSQL database encapsulated within a Docker container.

## DBT Model
![DBT Model](path/to/dbt-image.png)

The DBT model illustrates how staging and transformation tables are structured:
- **Staging Tables**: `stg_green_tripdata`, `stg_yellow_tripdata`
- **Dimension Tables**: `dim_zones`
- **Fact Table**: `fact_trips`
- The final model `dm_monthly_zone_revenue` presents aggregated insights.

## Dashboard Design
A Looker dashboard is utilized to visualize data insights, showcasing:
- **KPI Section**:
  - Total Trips
  - Total Revenue
  - Total Tips
  - Avg Fare
  - Avg Trip Distance
- **Trend Analysis**: Monthly trends in trips and revenue.
- **Geographic Insights**: Heatmaps and bar charts for zone-based exploration.
- **Customer Behavior**: Analysis of payment types and customer preferences.

## Installation Instructions
1. **Clone the Repository**: 
   ```bash
   git clone <repository-url>
   cd NYC-Taxi-Data-Engineering
