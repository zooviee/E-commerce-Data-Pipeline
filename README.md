# E-commerce-Data-Pipeline
Built ETL pipelines using Spark and Databricks; visualized insights in Power BI

## 🏗️ Architecture Overview

This project implements a modern Lakehouse architecture on Azure using Databricks, Unity Catalog, and ADLS, following the Medallion (Bronze → Silver → Gold) design pattern.

**Data Flow:**

1. **Source System (ShopVista)**

   * Transactional and master data is extracted as CSV files from the ShopVista system.

2. **Azure Data Lake Storage (ADLS)**

   * Acts as the centralized data lake.
   * Stores raw uploaded files and structured medallion layer data under a project root directory.
   * Organised into:

     * `raw/`
     * `bronze/`
     * `silver/`
     * `gold/`

3. **Secure Access via Access Connector**

   * Databricks accesses ADLS securely using an Azure Access Connector and secret scopes.
   * No credentials are hardcoded in notebooks.

4. **Azure Databricks + Unity Catalog**

   * Unity Catalog manages governance, permissions, and table organization.
   * Databricks notebooks implement all ETL/ELT logic:

     * **Bronze:** Raw structured ingestion
     * **Silver:** Cleansed and standardized datasets
     * **Gold:** Analytics-ready star schema (Dimensions + Facts)

5. **Medallion Architecture**

   * Bronze → Silver → Gold transformations ensure:

     * Data quality
     * Reproducibility
     * Analytics performance

6. **Analytics Layer (Power BI)**

   * Power BI connects directly to the Gold tables in Databricks.
   * Only curated business-ready tables are exposed to analysts and stakeholders.

<p align="center">
  <img src="docs/architecture/project_architecture.png" width="800"/>
</p>

---

## 📊 Power BI Analytics Report

The Power BI report consumes data from the **Gold layer** and provides end-to-end visibility into sales, customers, products, and operational performance.

### Key KPIs:

* **Total Sales:** 22.19bn
* **Units Sold:** 1.6M
* **Repeat Customer Rate:** 66.53%
* **Total Customers:** 299.7K
* **Average Discount %:** 8.67%
* **Highest Profit Region:** South

### Core Insights:

* Sales distribution by **region**, **brand**, and **category**
* Monthly revenue trends
* Mobile vs Website channel contribution
* Customer segmentation and behavior analysis
* High-performing brands and product categories

### Key Visuals:

* Customer Count by Region
* Total Sales by Channel
* Revenue Trend by Month
* Brand & Category Revenue Table
* KPI summary cards for executive dashboards

<p align="center">
  <img src="docs/screenshots/ecommerce_analytics_report.jpg" width="900"/>
</p>

---

## 🧠 Design Principles Demonstrated

* Medallion Lakehouse Architecture
* Secure cloud data access (no secrets in code)
* Dimensional modeling (Star Schema)
* Analytics-first Gold layer design
* Separation of:

  * Storage (ADLS)
  * Compute (Databricks)
  * Governance (Unity Catalog)
  * Visualization (Power BI)
