# 🚗 Car Sales Data Engineering Pipeline – Azure Project

## 📌 Project Overview
This project implements a full-scale data engineering pipeline on the Azure cloud, transforming raw car sales data into a star schema and enabling executive-level insights through Power BI dashboards.

Using a layered data architecture (Bronze → Silver → Gold), the pipeline demonstrates ingestion, transformation, modeling, and visualization of sales data, applying best practices in ETL, DevOps, and Agile.

---

## 🛠️ Technologies Used

- **Azure SQL Database** – For raw and processed data storage
- **Azure Data Factory (ADF)** – For building ETL pipelines and orchestration
- **Azure Databricks** – For data cleaning, transformation, and aggregation
- **GitHub** – Version control
- **Power BI** – For building interactive dashboards
- **SQL, PySpark, DAX** – For data processing and modeling

---

## 📊 Data Flow Architecture

### 🟤 Bronze Layer
- **Source**: Raw car sales data from GitHub - https://github.com/anshlambagit/Azure-DE-Project-Resources/tree/main/Raw%20Data
- **Ingestion**: Performed incremental loads using ADF pipeline
- **ADF Components**: Pipelines, Linked Services, Parameters, Stored Procedures

### ⚪ Silver Layer
- **Platform**: Azure Databricks (PySpark)
- **Transformations**: Cleaned and joined data from multiple sources into a consolidated table
- **Notebook Visuals**: Exploratory insights and sanity checks

### 🟡 Gold Layer
- **Star Schema Modeling**: Fact and Dimension tables built from Silver data
- **Concepts Applied**: Slowly Changing Dimensions (Type 1), surrogate keys
- **ETL**: Final pipeline to publish curated data to Azure SQL

---

## 📈 Power BI Dashboards

1. **Executive Overview**
   - Total Revenue, Units Sold, Average Price KPIs
   - Trend over time with YoY growth
     <img width="1275" height="717" alt="image" src="https://github.com/user-attachments/assets/f6d60c2f-0562-42b8-9729-08a8bde15a33" />


2. **Branch & Dealer Analysis**
   - Top branch and dealer KPIs
   - Revenue by Branch and Dealer
   - Units sold over time
   - Drilldowns and filters
     <img width="1278" height="715" alt="image" src="https://github.com/user-attachments/assets/51119626-79e0-47dc-b29d-b8d10cadacf8" />


3. **Model & Category Overview**
   - Top model and category KPIs
   - Revenue and units sold by model category
   - Price vs. sales volume scatter plot
   - Revenue per unit analysis
<img width="1277" height="717" alt="image" src="https://github.com/user-attachments/assets/42a012d0-c6f6-47fa-8f91-27e2efb92c07" />

---

## 📁 Folder Structure (optional if code shared)
 - notebooks/ # Azure Databricks Notebooks
 - powerbi/ # PBIX Dashboard Files
 - adf_pipelines/ # JSON exports of ADF pipelines
 - sql/ # Schema scripts, procedures
 - README.md # Project overview

