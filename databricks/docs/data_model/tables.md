# 🥇 Gold Layer (Analytics Ready)

The Gold layer contains curated dimension and fact tables designed for analytics and reporting.
These tables power Power BI dashboards and support business metrics.


# gld_dim_customers

Grain: One row per customer
- Primary Key: customer_id

| Column      | Type   | Description                |
| ----------- | ------ | -------------------------- |
| customer_id | string | Unique customer identifier |
| country     | string | Customer country           |
| state       | string | Customer state             |
| region      | string | Customer region            |
| phone       | string | Customer contact number    |

Purpose:
Stores customer master data for segmentation and geographic analysis.


# gld_dim_products

Grain: One row per product
- Primary Key: product_id

| Column        | Type   | Description               |
| ------------- | ------ | ------------------------- |
| product_id    | string | Unique product identifier |
| sku           | string | Stock keeping unit        |
| category_name | string | Product category          |
| brand_name    | string | Product brand             |
| color         | string | Product color             |
| size          | string | Product size              |
| material      | string | Product material          |

Purpose: 
Provides product attributes for sales and inventory analytics.


# gld_dim_date

Grain: One row per date
- Primary Key: date_id

| Column     | Type   | Description        |
| ---------- | ------ | ------------------ |
| date_id    | int    | Surrogate date key |
| date       | date   | Calendar date      |
| year       | int    | Year               |
| month_name | string | Month name         |
| day_name   | string | Day of week        |
| is_weekend | int    | Weekend indicator  |

Purpose: 
Standard time dimension for all fact tables.


# gld_fact_order_items

Grain: One row per order item
- Foreign Keys:
-- product_id → gld_dim_products
-- customer_id → gld_dim_customers
-- date_id → gld_dim_date

| Column          | Type   | Description            |
| --------------- | ------ | ---------------------- |
| transaction_id  | int    | Unique transaction ID  |
| product_id      | bigint | Product reference      |
| customer_id     | string | Customer reference     |
| quantity        | int    | Units sold             |
| unit_price      | double | Price per unit         |
| gross_amount    | double | Total before discounts |
| discount_amount | bigint | Discount applied       |
| tax_amount      | int    | Tax charged            |
| net_amount      | double | Final payable amount   |

Purpose:
Core sales fact table used for revenue, volume, and discount analysis.


# gld_fact_daily_orders_summary

Grain: One row per date and currency

| Column                | Type   | Description                  |
| --------------------- | ------ | ---------------------------- |
| date_id               | int    | Date reference               |
| currency              | string | Transaction currency         |
| total_quantity        | bigint | Total items sold             |
| total_gross_amount    | double | Total sales before discounts |
| total_discount_amount | bigint | Total discounts              |
| total_tax_amount      | bigint | Total tax collected          |
| total_amount          | double | Final revenue                |

Purpose:
Optimized for KPI dashboards and trend analysis.


# gld_fact_order_returns

Grain: One row per returned order
Composite Keys:
- order_id 
- order_dt 
- return_ts

| Column         | Type      | Description                   |
| -------------- | --------- | ----------------------------- |
| order_id       | int       | Order identifier              |
| order_dt       | date      | Order date                    |
| return_ts      | timestamp | Return timestamp              |
| return_days    | int       | Days between order and return |
| within_policy  | int       | Return within policy flag     |
| is_late_return | int       | Late return indicator         |
| reason         | string    | Return reason                 |

Purpose:
Supports return behavior analysis and operational efficiency metrics.


# gld_fact_order_shipments

Grain: One row per shipment
- Composite Keys:
-- order_id
-- shipment_id 

| Column              | Type    | Description           |
| ------------------- | ------- | --------------------- |
| order_id            | int     | Order identifier      |
| shipment_id         | string  | Shipment identifier   |
| carrier             | string  | Shipping carrier      |
| carrier_group       | string  | Carrier category      |
| is_weekend_shipment | boolean | Weekend shipment flag |

Purpose:
Used for logistics and delivery performance analytics.



# Silver Layer

| Table               | Description |
|-------------------- |----------------------------------------------------|
| slv_customers       | Cleaned and standardized customer master data      |
| slv_products        | Cleaned product dimension source                   |
| slv_order_items     | Transaction-level order items after deduplication  |
| slv_order_returns   | Returns after validation and normalization         |
| slv_order_shipments | Shipments after validation and normalization       |




# Bronze Layer
Bronze tables store raw ingested data with minimal transformation.  
They preserve source fidelity and support reprocessing.

Tables:
- brz_customers
- brz_products
- brz_order_items
- brz_order_returns
- brz_order_shipments
- brz_brands
- brz_category
- brz_calendar
