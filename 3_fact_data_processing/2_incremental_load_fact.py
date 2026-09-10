# Databricks notebook source
# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

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
print("Processed path: ", processed_path)

# Define the tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Bronze Processing

# COMMAND ----------

df = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(landing_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

print("Total Rows: ", df.count())
display(df.limit(5))

# COMMAND ----------

df.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .mode("append")\
                .saveAsTable(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Staging table to process justthe arrived incremental data

# COMMAND ----------

df.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .mode("overwrite")\
                .saveAsTable(f"{catalog}.{bronze_schema}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Moving files from source to processed directory in S3

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
# MAGIC ###Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.staging_{data_source}")
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

# 1. Keep only rows where order_qty is not null
df_silver = df_bronze.filter(F.col("order_qty").isNotNull())

# COMMAND ----------

# 2. Clean customer_id -> keep numeric, else set to 999999
df_silver = df_silver.withColumn(
    "customer_id",
    F.when(
        F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id")
    )
    .otherwise(999999).cast("string")
)

# COMMAND ----------

# 3.Remove weekday name from the date text
df_silver = df_silver.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# COMMAND ----------

# 4. Parse order_placement_date using multiple possible formats
df_silver = df_silver.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date(F.col("order_placement_date"), "yyyy/MM/dd"),
        F.try_to_date(F.col("order_placement_date"), "yyyy-MM-dd"),
        F.try_to_date(F.col("order_placement_date"), "dd/MM/yyyy"),
        F.try_to_date(F.col("order_placement_date"), "dd-MM-yyyy"),
        F.try_to_date(F.col("order_placement_date"), "MMMM dd, yyyy")
    )
)

# COMMAND ----------

# 5. Drop Duplicates
df_silver = df_silver.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# 6. Convert product_id to string
df_silver = df_silver.withColumn(
    "product_id",
    F.col("product_id").cast("string")
)


# COMMAND ----------

# Check what's the maximum and minimum date
df_silver.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join with products

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")
df_joined = df_silver.join(
    df_products,
    on="product_id",
    how="inner" ).select(df_silver["*"], df_products["product_code"])

display(df_joined.limit(5))


# COMMAND ----------

# save the staging table if silver table does not exist, otherwise insert or update the silver table

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option("delta.enableChangeDataFeed", "true").option("mergeSchema","true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("target").merge(
        source = df_joined.alias("source"),
        condition = "target.order_id = source.order_id AND target.order_placement_date = source.order_placement_date AND target.customer_id = source.customer_id AND target.product_code = source.product_code"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Staging table to process just the arrived incremental data

# COMMAND ----------

# Staging for incremental data

df_joined.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .mode("overwrite")\
                .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Processing

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {catalog}.{silver_schema}.staging_{data_source}")
display(df_gold.limit(2))

# COMMAND ----------

df_gold.count()

# COMMAND ----------

gold_table

# COMMAND ----------

# Merge the data in staging_silver_table into silver_table
if not (spark.catalog.tableExists(gold_table)):
    print("Creating New Table")
    df_gold.write.format("delta").option("delta.enableChangeDataFeed", "true").option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("target").merge(
        source = df_gold.alias("source"),
        condition = "target.order_id = source.order_id AND target.date = source.date AND target.customer_code = source.customer_code AND target.product_code = source.product_code"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Merging with Parent company

# COMMAND ----------

# MAGIC %md
# MAGIC - Note: We want data for monthly level but child data is on daily level

# COMMAND ----------

# MAGIC %md
# MAGIC ######Incremental Load

# COMMAND ----------

# df_child = incremental daily rows

display(spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.staging_{data_source} ").limit(20))

# COMMAND ----------

df_child = spark.sql(f"SELECT order_placement_date as date FROM {catalog}.{silver_schema}.staging_{data_source}")

# This will return the distinct dates after converting each date into the first day of the month
incremental_month_df = df_child.select(
    F.trunc("date", "MM").alias("start_month")
).distinct()

incremental_month_df.show()

# Create a temp table for incremental_month_df
incremental_month_df.createOrReplaceTempView("incremental_months")

# COMMAND ----------

monthly_table = spark.sql(f"""
                          SELECT date, product_code, customer_code, sold_quantity
                          FROM {catalog}.{gold_schema}.sb_fact_orders sbf
                          INNER JOIN incremental_months m
                          ON trunc(sbf.date, "MM") = m.start_month
                          """)
print(monthly_table.count())
display(monthly_table.limit(10))

# COMMAND ----------

monthly_table.select('date').distinct().orderBy('date').show()

# COMMAND ----------

df_monthly_recalc = (
    monthly_table
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date") # month_start → date = first of month
)

display(df_monthly_recalc.limit(10))

# COMMAND ----------

df_monthly_recalc.count()

# COMMAND ----------

# Merge the monthly_recalc data into parent orders table
gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")

gold_parent_delta.alias("target").merge(
    source= df_monthly_recalc.alias("source"),
    condition=  "target.date = source.date AND target.product_code = source.product_code AND target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.bronze.staging_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.silver.staging_orders;