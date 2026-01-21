# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Fact Data into Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Order Items

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "ecommcistorage05", "Storage Account Name")
dbutils.widgets.text("container_name", "ecomm-raw-data", "Container Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

print(catalog_name, storage_account_name, container_name)

# COMMAND ----------

# MAGIC %md
# MAGIC We created the widget so when we want to run the same notebook in different environment (dev, test, prod), we can easily do that. Instead of changing code cells

# COMMAND ----------

# -------------------------
# Azure Data Lake Storage - ADLS Gen2
# -------------------------

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/order_items/landing/"

# Checkpoint folders for streaming (bronze, silver, gold)
bronze_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_items/"


# COMMAND ----------

# MAGIC %md
# MAGIC 1. We are using `Autoloader` to perform incremental data processing.
# MAGIC 2.`Bronze` layer is a data sink so it is append only. There are no updates on delete in the bronze layer.
# MAGIC 3. `Silver` layer will take care of deduplications on duplicate files
# MAGIC 4. `Gold` layer has analytics ready table
# MAGIC 5. trigger(availableNow=True) is used to perform batch operation. You can stream the data as well if you have continuously arriving data and low letency requirements are there.

# COMMAND ----------

spark.readStream \
 .format("cloudFiles") \
 .option("cloudFiles.format", "csv")  \
 .option("cloudFiles.schemaLocation", bronze_checkpoint_path) \
 .option("cloudFiles.schemaEvolutionMode", "rescue") \
 .option("header", "true") \
 .option("cloudFiles.inferColumnTypes", "true") \
 .option("rescuedDataColumn", "_rescued_data") \
 .option("cloudFiles.includeExistingFiles", "true")  \
 .option("pathGlobFilter", "*.csv") \
 .load(adls_path) \
 .withColumn("ingest_timestamp", F.current_timestamp()) \
 .withColumn("source_file", F.col("_metadata.file_path")) \
 .writeStream \
 .outputMode("append") \
 .option("checkpointLocation", bronze_checkpoint_path) \
 .trigger(availableNow=True) \
 .toTable(f"{catalog_name}.bronze.brz_order_items") \
 .awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT max(dt) FROM {catalog_name}.bronze.brz_order_items"))
display(spark.sql(f"SELECT min(dt) FROM {catalog_name}.bronze.brz_order_items"))

# COMMAND ----------

display(
    spark.sql(
        f"SELECT count(*) FROM CLOUD_FILES_STATE('abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_items/')"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Order Returns

# COMMAND ----------

# -------------------------
# Azure Data Lake Storage - ADLS Gen2
# -------------------------

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/order_returns/landing/"

# Checkpoint folders for streaming (bronze, silver, gold)
bronze_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_returns/"

# COMMAND ----------

spark.readStream \
 .format("cloudFiles") \
 .option("cloudFiles.format", "csv")  \
 .option("cloudFiles.schemaLocation", bronze_checkpoint_path) \
 .option("cloudFiles.schemaEvolutionMode", "rescue") \
 .option("header", "true") \
 .option("cloudFiles.inferColumnTypes", "true") \
 .option("rescuedDataColumn", "_rescued_data") \
 .option("cloudFiles.includeExistingFiles", "true")  \
 .option("pathGlobFilter", "*.csv") \
 .load(adls_path) \
 .withColumn("ingest_timestamp", F.current_timestamp()) \
 .withColumn("source_file", F.col("_metadata.file_path")) \
 .writeStream \
 .outputMode("append") \
 .option("checkpointLocation", bronze_checkpoint_path) \
 .trigger(availableNow=True) \
 .toTable(f"{catalog_name}.bronze.brz_order_returns") \
 .awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT max(order_dt) FROM {catalog_name}.bronze.brz_order_returns"))
display(spark.sql(f"SELECT min(order_dt) FROM {catalog_name}.bronze.brz_order_returns"))

# COMMAND ----------

display(
    spark.sql(
        f"SELECT count(*) FROM CLOUD_FILES_STATE('abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_returns/')"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Order Shipments

# COMMAND ----------

# -------------------------
# Azure Data Lake Storage - ADLS Gen2
# -------------------------

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/order_shipments/landing/"

# Checkpoint folders for streaming (bronze, silver, gold)
bronze_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_shipments/"

# COMMAND ----------

spark.readStream \
 .format("cloudFiles") \
 .option("cloudFiles.format", "csv")  \
 .option("cloudFiles.schemaLocation", bronze_checkpoint_path) \
 .option("cloudFiles.schemaEvolutionMode", "rescue") \
 .option("header", "true") \
 .option("cloudFiles.inferColumnTypes", "true") \
 .option("rescuedDataColumn", "_rescued_data") \
 .option("cloudFiles.includeExistingFiles", "true")  \
 .option("pathGlobFilter", "*.csv") \
 .load(adls_path) \
 .withColumn("ingest_timestamp", F.current_timestamp()) \
 .withColumn("source_file", F.col("_metadata.file_path")) \
 .writeStream \
 .outputMode("append") \
 .option("checkpointLocation", bronze_checkpoint_path) \
 .trigger(availableNow=True) \
 .toTable(f"{catalog_name}.bronze.brz_order_shipments") \
 .awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT max(order_dt) FROM {catalog_name}.bronze.brz_order_shipments"))
display(spark.sql(f"SELECT min(order_dt) FROM {catalog_name}.bronze.brz_order_shipments"))

# COMMAND ----------

display(
    spark.sql(
        f"SELECT count(*) FROM CLOUD_FILES_STATE('abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_shipments/')"
    )
)