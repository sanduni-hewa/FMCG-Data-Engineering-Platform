# Databricks notebook source
# MAGIC %md
# MAGIC ####Import required Libraries

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #####Load Project Utilities & Initalize Notebook Widgets
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/sandurash19@gmail.com/FMCG_Project/FMCG_Pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://sportsbar-sr123/{data_source}/*.csv"
print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Bronze Processing

# COMMAND ----------

df_bronze = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #####

# COMMAND ----------

df_bronze.write\
  .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
      .mode("overwrite")\
        .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC - **Normalise month field**

# COMMAND ----------

df_bronze.select('month').distinct().show()

# COMMAND ----------

# Parse 'month' from multiple possible formats
date_formats = ['yyyy/MM/dd', 'dd/MM/yyyy', 'yyyy-MM-dd', 'dd-MM-yyyy']

df_silver = df_bronze.withColumn(
    "month",
    F.coalesce(
        *[F.try_to_date(F.col("month"), fmt) for fmt in date_formats]
    )
)

df_silver.select("month").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - **Handling gross_price**

# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

# We are validating the gross_price column, converting only valid numeric values to double, fixing negative price by making them positive, and replacing all non-numeric values with 0

df_silver = df_silver.withColumn(
    "gross_price",
    F.when(
        F.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'),
        F.when(
            F.col("gross_price").cast("double") < 0, -1 * F.col("gross_price").cast("double")
        ).otherwise(F.col("gross_price").cast("double"))
    ).otherwise(0)
)

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC - **Pulling product_code into pricing table**

# COMMAND ----------

# We enrich the silver datasets by performing an inner join with the product table to fetch the correct product_code for each product_id

df_products = spark.table("fmcg.silver.products")

# Check how many distinct product ids are present in the silver table
print(df_silver.select("product_id").distinct().count())

# Check how many distinct product codes are present in the products table
print(df_products.select("product_code").distinct().count())

# COMMAND ----------

df_joined = df_silver.join(df_products.select("product_id", "product_code"), on="product_id", how="inner")

df_joined = df_joined.select("product_id", "product_code", "month", "gross_price", "read_timestamp", "file_name", "file_size")
display(df_joined.limit(10))

# COMMAND ----------

df_joined.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .option("mergeSchema", "true")\
                .mode("overwrite")\
                    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source}")


# COMMAND ----------

# Select only required columns
df_gold = df_silver.select("product_code", "month", "gross_price")

df_gold.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .mode("overwrite")\
                .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")




# COMMAND ----------

# MAGIC %md
# MAGIC ####Merging Data Source with Parent

# COMMAND ----------

df_gold = spark.sql(f"SELECT * FROM {catalog}.{gold_schema}.sb_dim_{data_source}")
df_gold.show(5)

df_parent_pricing = spark.sql(f"SELECT * FROM {catalog}.{gold_schema}.dim_gross_price")
df_parent_pricing.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC - **Get the price for each product_code (Aggregated by year)**

# COMMAND ----------

df_gold = (
  df_gold
  .withColumn("year", F.year("month"))
  # 0 = non-zero price, 1 = zero price -> non-zero comes first
  .withColumn("is_zero", F.when(F.col("gross_price") == 0, 1).otherwise(0))
)

w = (
  Window
    .partitionBy("product_code", "year")
    .orderBy(F.col("is_zero"), F.col("month").desc())
)

df_gold_latest_price = (
  df_gold
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk")==1)
  )


# COMMAND ----------

display(df_gold_latest_price)

# COMMAND ----------

# Take required columns
df_gold_latest_price = df_gold_latest_price.select("product_code", "year", "gross_price").withColumnRenamed("gross_price", "price_inr").select("product_code", "price_inr", "year")
df_gold_latest_price.show(5)

# COMMAND ----------

df_gold_latest_price = df_gold_latest_price.withColumn("year", F.col("year").cast("string"))
df_gold_latest_price.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - **Merging Data source with Parent**

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")

delta_table.alias("target").merge(
    source = df_gold_latest_price.alias("source"),
    condition = "target.product_code = source.product_code"
).whenMatchedUpdate(
    set = {
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values = {
        "product_code": "source.product_code",
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).execute()