# Databricks notebook source
# MAGIC %md
# MAGIC ####Import required Libraries

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #####Load Project Utilities & Initalize Notebook Widgets

# COMMAND ----------

# MAGIC %run /Workspace/Users/sandurash19@gmail.com/FMCG_Project/FMCG_Pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://sportsbar-sr123/{data_source}"
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)

# Define the table
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Bronze Processing

# COMMAND ----------

df = ( 
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(landing_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.File_name", "_metadata.file_size")
)

print("Total rows: ", df.count())


# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("append")\
    .saveAsTable(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {bronze_table}")
display(df_bronze.limit(20))

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
print(files)

# COMMAND ----------

for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #####Transformation

# COMMAND ----------

# Keep only rows where order_qty is present

df_bronze = df_bronze.filter(F.col("order_qty").isNotNull())
display(df_bronze)

# COMMAND ----------

# Clean customer_id -> Keep numberic, else set to 999999

df_bronze = df_bronze.withColumn(
    "customer_id",
    F.when(
        F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id")
    ).otherwise("999999").cast("string")
)

# COMMAND ----------

# Normalise order_placement_date

display(df_bronze.select("order_placement_date").distinct())

# COMMAND ----------

# Remove weekday name from the date text
# "Saturday, July 05, 2025" -> "July 05, 2025"

df_bronze = df_bronze.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# COMMAND ----------

display(df_bronze.select("order_placement_date").distinct())

# COMMAND ----------

# Parse order_placement_date using multiple possible formats

df_bronze = df_bronze.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date(F.col("order_placement_date"), "yyyy/MM/dd"),
        F.try_to_date(F.col("order_placement_date"), "dd/MM/yyyy"),
        F.try_to_date(F.col("order_placement_date"), "yyyy-MM-dd"),
        F.try_to_date(F.col("order_placement_date"), "dd-MM-yyyy"),
        F.try_to_date(F.col("order_placement_date"), "MMMM dd, yyyy")
    )
)

# COMMAND ----------

# Drop duplicates

df_bronze = df_bronze.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# COMMAND ----------

# Convert product_id to string

df_bronze = df_bronze.withColumn("product_id", F.col("product_id").cast("string"))

# COMMAND ----------

display(df_bronze.limit(20))

# COMMAND ----------

# Check what's the minimum and maximum date
df_bronze.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

# joing with product_table to get product_code

df_products = spark.table("fmcg.silver.products")
display(df_products.limit(5))

# COMMAND ----------

df_joined = df_bronze.join(df_products, on="product_id", how="inner").select(df_bronze["*"], df_products["product_code"])

df_joined.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Save the fact table into silver schema

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    print("Creating New Table")
    df_joined.write.format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .option("mergeSchema", "true")\
                .mode("overwrite").saveAsTable(silver_table)
else:
    print("Appending to existing table")
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("target").merge(
        source = df_joined.alias("source"),
        condition = "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table}")
df_silver.show(5)

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_silver.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(
        df_silver.alias("gold"), 
        "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Merge with Parent Company

# COMMAND ----------

# MAGIC %md
# MAGIC - Note: We want data for monthly level but child data is on daily level

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")
df_child.show(5)

# COMMAND ----------

df_child.count()

# COMMAND ----------

df_monthly = (
    df_child
    # 1. Get month start date (e.g., 2025-11-30 → 2025-11-01)
    .withColumn("month_start", F.trunc("date", "MM"))   # or F.date_trunc("month", "date").cast("date")

    # 2.Group at monthly grain by month_start + product_code + customer_code
    .groupBy("month_start", "product_code", "customer_code")
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )

    # 3. Rename month_start back to `date` to match your target schema
    .withColumnRenamed("month_start", "date")
)

df_monthly.show(5, truncate=False)

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")

gold_parent_delta.alias("parent_gold").merge(
    source = df_monthly.alias("child_gold"), 
    condition = "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()