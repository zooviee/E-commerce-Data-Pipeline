# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver: Data Cleansing and Transformation

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Widgets

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "ecommcistorage05", "Storage Account Name")
dbutils.widgets.text("container_name", "ecomm-raw-data", "Container Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Bronze Table in a Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Order Items

# COMMAND ----------

# DBTITLE 1,Untitled
df = spark.readStream \
.format("delta") \
.table(f"{catalog_name}.bronze.brz_order_items")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform Transformations and Cleaning

# COMMAND ----------

df = df.dropDuplicates(["order_id", "item_seq"])

# Transformation: Convert 'Two' → 2 and cast to Integer
df = df.withColumn(
    "quantity",
    F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int")
)

# Transformation : Remove any '$' or other symbols from unit_price, keep only numeric
df = df.withColumn(
    "unit_price",
    F.regexp_replace("unit_price", "[$]", "").cast("double")
)

# Transformation : Remove '%' from discount_pct and cast to double
df = df.withColumn(
    "discount_pct",
    F.regexp_replace("discount_pct", "%", "").cast("double")
)

# Transformation : coupon code processing (convert to lower)
df = df.withColumn(
    "coupon_code", F.lower(F.trim(F.col("coupon_code")))
)

# Transformation : channel processing 
df = df.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
    .when(F.col("channel") == "app", "Mobile")
    .otherwise(F.col("channel")),
)

#Transformation : Add processed time 
df = df.withColumn(
    "processed_time", F.current_timestamp()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Silver Table

# COMMAND ----------

silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_items/"
print(silver_checkpoint_path)

# COMMAND ----------

def upsert_to_silver(microBatchDF, batchId):
    table_name = f"{catalog_name}.silver.slv_order_items"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("silver_table").merge(
            microBatchDF.alias("batch_table"),
            "silver_table.order_id = batch_table.order_id AND silver_table.item_seq = batch_table.item_seq",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()    

    



# COMMAND ----------

# This line is running a Structured Streaming job that:
# - Reads incremental data from Bronze (df).
# - For each batch → applies upsert_to_silver (update if exists, insert if not).
# - Writes into a Silver Delta table with schema evolution enabled.
# - Uses checkpointing for recovery.
# - Runs in batch-like mode (once or availableNow), not continuous streaming.

df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Order Returns

# COMMAND ----------

# Or just column names
spark.table(f"{catalog_name}.bronze.brz_order_returns").columns

# COMMAND ----------

# Checkpoint path
silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/slv_order_returns/"

# Silver table name
silver_table_name = f"{catalog_name}.silver.slv_order_returns"

# ============================================
# 1. READ & TRANSFORM BRONZE DATA
# ============================================
transformed_df = (
    spark.readStream
    .table(f"{catalog_name}.bronze.brz_order_returns")
    
    # Convert order_dt to DateType
    .withColumn("order_dt", F.to_date(F.col("order_dt")))
    
    # Convert return_ts to TimestampType
    .withColumn("return_ts", F.to_timestamp(F.col("return_ts")))
    
    # Clean reason column (uppercase + trim)
    .withColumn("reason", F.upper(F.trim(F.col("reason"))))
    
    # Add processing timestamp
    .withColumn("processed_time", F.current_timestamp())
    
    # Remove duplicates within micro-batch
    .dropDuplicates(["order_id", "order_dt", "return_ts"])
    
    # Select only needed columns (remove Bronze metadata)
    .select(
        "order_id",
        "order_dt",
        "return_ts",
        "reason",
        "processed_time"
    )
)

# COMMAND ----------

# ============================================
# 2. UPSERT FUNCTION
# ============================================
def upsert_to_silver(microBatchDF, batchId):
    """
    Upsert micro-batch data to Silver table.
    Creates table if not exists, otherwise merges.
    """
    
    # Skip empty batches
    if microBatchDF.isEmpty():
        print(f"Batch {batchId}: Empty batch, skipping...")
        return
    
    print(f"Batch {batchId}: Processing {microBatchDF.count()} records")
    
    if not spark.catalog.tableExists(silver_table_name):
        # Create new table
        print(f"Batch {batchId}: Creating new table {silver_table_name}")
        (
            microBatchDF.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table_name)
        )
        # Enable Change Data Feed
        spark.sql(f"""
            ALTER TABLE {silver_table_name} 
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
    else:
        # Merge into existing table
        print(f"Batch {batchId}: Merging into {silver_table_name}")
        deltaTable = DeltaTable.forName(spark, silver_table_name)
        
        (
            deltaTable.alias("target")
            .merge(
                microBatchDF.alias("source"),
                """
                target.order_id = source.order_id AND 
                target.order_dt = source.order_dt AND 
                target.return_ts = source.return_ts
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    print(f"Batch {batchId}: Complete")


# COMMAND ----------

# ============================================
# 3. WRITE STREAM WITH UPSERT
# ============================================
(
    transformed_df
    .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", silver_checkpoint_path)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Order Shipments

# COMMAND ----------

# Or just column names
spark.table(f"{catalog_name}.bronze.brz_order_shipments").columns

# COMMAND ----------

# Checkpoint path
silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_shipments/"

# Silver table name
silver_table_name = f"{catalog_name}.silver.slv_order_shipments"

# ============================================
# 1. READ & TRANSFORM BRONZE DATA
# ============================================
transformed_df = (
    spark.readStream
    .table(f"{catalog_name}.bronze.brz_order_shipments")
    
    # Convert order_dt to DateType
    .withColumn("order_dt", F.to_date(F.col("order_dt")))

    
    # Clean carrier column (uppercase + trim)
    .withColumn("carrier", F.upper(F.trim(F.col("carrier"))))
    
    # Add processing timestamp
    .withColumn("processed_time", F.current_timestamp())
    
    # Select only needed columns (remove Bronze metadata)
    .select(
        "order_id",
        "order_dt",
        "shipment_id",
        "carrier",
        "processed_time"
    )
)

# COMMAND ----------

# ============================================
# 2. UPSERT FUNCTION
# ============================================
def upsert_to_silver(microBatchDF, batchId):
    """
    Upsert micro-batch data to Silver table.
    Creates table if not exists, otherwise merges.
    """
    
    # Skip empty batches
    if microBatchDF.isEmpty():
        print(f"Batch {batchId}: Empty batch, skipping...")
        return
    
    print(f"Batch {batchId}: Processing {microBatchDF.count()} records")
    
    if not spark.catalog.tableExists(silver_table_name):
        # Create new table
        print(f"Batch {batchId}: Creating new table {silver_table_name}")
        (
            microBatchDF.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table_name)
        )
        # Enable Change Data Feed
        spark.sql(f"""
            ALTER TABLE {silver_table_name} 
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
    else:
        # Merge into existing table
        print(f"Batch {batchId}: Merging into {silver_table_name}")
        deltaTable = DeltaTable.forName(spark, silver_table_name)
        
        (
            deltaTable.alias("target")
            .merge(
                microBatchDF.alias("source"),
                """
                target.order_id = source.order_id
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    print(f"Batch {batchId}: Complete")


# COMMAND ----------

# ============================================
# 3. WRITE STREAM WITH UPSERT
# ============================================
(
    transformed_df
    .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", silver_checkpoint_path)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)