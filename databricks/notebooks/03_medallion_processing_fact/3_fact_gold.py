# Databricks notebook source
# MAGIC %md
# MAGIC # From Silver To Gold: Aggregation and KPI Tables

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
from pyspark.sql.functions import col, when, dayofweek, current_timestamp
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "ecommcistorage05", "Storage Account Name")
dbutils.widgets.text("container_name", "ecomm-raw-data", "Container Name")

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure to include your `storage account name` in the widget section at the top of the notebook

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

print(catalog_name, storage_account_name, container_name)

# COMMAND ----------

# DBTITLE 1,Cell 7
# readChangeFeed flag is used to read the change feed (_change_type column mainly)
df = spark.readStream \
.format("delta") \
.option("readChangeFeed", "true") \
.table(f"{catalog_name}.silver.slv_order_items")

# COMMAND ----------

df_union = df.filter("_change_type IN ('insert', 'update_postimage')")

# COMMAND ----------

df_union = df_union.withColumn(
    "gross_amount",
    F.col("quantity") * F.col("unit_price")
    )

# 2) Add discount_amount (discount_pct is already numeric, e.g., 21 -> 21%)
df_union = df_union.withColumn(
    "discount_amount",
    F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0))
)

# 3) Add sale_amount = gross - discount
df_union = df_union.withColumn(
    "sale_amount",
    F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
)

# add date id
df_union = df_union.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))  # Create date_key

# Coupon flag
#  coupon flag = 1 if coupon_code is not null else 0
df_union = df_union.withColumn(
    "coupon_flag",
    F.when(F.col("coupon_code").isNotNull(), F.lit(1))
     .otherwise(F.lit(0))
)   

# COMMAND ----------

orders_gold_df = df_union.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold/fact_order_items/"
print(gold_checkpoint_path)

def upsert_to_gold(microBatchDF, batchId):
    table_name = f"{catalog_name}.gold.gld_fact_order_items"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("gold_table").merge(
            microBatchDF.alias("batch_table"),
            "gold_table.transaction_id = batch_table.transaction_id AND gold_table.seq_no = batch_table.seq_no",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

orders_gold_df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()

# COMMAND ----------

spark.sql(f"SELECT count(*) FROM {catalog_name}.gold.gld_fact_order_items").show()

# COMMAND ----------

spark.sql(f"SELECT max(transaction_date) FROM {catalog_name}.gold.gld_fact_order_items").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Order Returns

# COMMAND ----------

# Paths and table name
gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold/fact_order_returns/"
gold_table_name = f"{catalog_name}.gold.gld_fact_order_returns"

# ============================================
# 1. READ & TRANSFORM SILVER DATA
# ============================================
transformed_df = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .table(f"{catalog_name}.silver.slv_order_returns")
    

# Filter out delete operations (keep inserts and updates)
df_union = transformed_df.filter(col("_change_type").isin("insert", "update_postimage"))
    
# Drop CDF metadata columns
df_union =  df_union.drop("_change_type", "_commit_version", "_commit_timestamp")


# Add date_id column (yyyyMMdd format)
df_union = df_union.withColumn("date_id", F.date_format(F.col("order_dt"), "yyyyMMdd").cast("int"))
    
# Calculate return_days
df_union = df_union.withColumn("return_days", F.datediff(F.col("return_ts"), F.col("order_dt")))
    
# Policy compliance flags
df_union = df_union.withColumn("within_policy", F.when(F.col("return_days") <= 15, 1).otherwise(0))


df_union = df_union.withColumn("is_late_return", F.when(F.col("return_days") > 15, 1).otherwise(0))
    
# Select final columns
returns_gold_df =  df_union.select(
        F.col("order_id"),
        F.col("date_id"),
        F.col("order_dt"),
        F.col("return_ts"),
        F.col("return_days"),
        F.col("reason"),
        F.col("within_policy"),
        F.col("is_late_return"),
        F.col("processed_time")
    )

# ============================================
# 2. UPSERT FUNCTION
# ============================================
def upsert_to_gold(microBatchDF, batchId):
    table_name = f"{catalog_name}.gold.gld_fact_order_returns"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("gold_table").merge(
            microBatchDF.alias("batch_table"),
            "gold_table.order_id = batch_table.order_id AND \
             gold_table.order_dt = batch_table.order_dt AND \
             gold_table.return_ts = batch_table.return_ts" 
            
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# ============================================
# 3. WRITE STREAM
# ============================================
returns_gold_df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Order Shipments

# COMMAND ----------

# Paths and table name
gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold fact_order_shipments/"
gold_table_name = f"{catalog_name}.gold.gld_fact_order_shipments"


# Define domestic carriers
domestic_carriers = ["ECOMEXPRESS", "DELHIVERY", "XPRESSBEES", "BLUEDART"]


# ============================================
# 1. READ & TRANSFORM SILVER DATA
# ============================================
transformed_df = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .table(f"{catalog_name}.silver.slv_order_shipments")

# Filter out delete operations (keep inserts and updates)
df_union = transformed_df.filter(col("_change_type").isin("insert", "update_postimage"))
    
# Drop CDF metadata columns
df_union =  df_union.drop("_change_type", "_commit_version", "_commit_timestamp")

    
# Add carrier_group column
df_union =  df_union.withColumn(
        "carrier_group",
        when(col("carrier").isin(domestic_carriers), "Domestic")
        .otherwise("International")
    )
    
# Add is_weekend_shipment flag
df_union = df_union.withColumn(
        "is_weekend_shipment",
        when(dayofweek(col("order_dt")).isin(1, 7), True)
        .otherwise(False)
    )
# Select final columns
shipments_gold_df = df_union.select(
        F.col("order_id"),
        F.col("order_dt"),
        F.col("shipment_id"),
        F.col("carrier"),
        F.col("carrier_group"),
        F.col("is_weekend_shipment")
    )


# ============================================
# 2. UPSERT FUNCTION
# ============================================
def upsert_to_gold(microBatchDF, batchId):
    table_name = f"{catalog_name}.gold.gld_fact_order_shipments"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("gold_table").merge(
            microBatchDF.alias("batch_table"),
            "gold_table.order_id = batch_table.order_id"
            
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# ============================================
# 3. WRITE STREAM
# ============================================
shipments_gold_df.writeStream.trigger(availableNow=True).foreachBatch(upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()
