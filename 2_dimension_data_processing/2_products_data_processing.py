# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ######Load Project Utilities & Initialize Notebook Widgets

# COMMAND ----------

# MAGIC %run /Workspace/Users/sandurash19@gmail.com/FMCG_Project/FMCG_Pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://sportsbar-sr123/{data_source}/*.csv"

print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Processing

# COMMAND ----------

# Read the data from S3 buckets

df = (
  spark.read.format("csv")
  .option("header", True)
  .option("inferSchema", True)
  .load(base_path)
  .withColumn("read_timestamp", F.current_timestamp())
  .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

df.printSchema()
display(df.limit(10))

# COMMAND ----------

# Save the dataframe to the bronze schema
df.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .mode("overwrite")\
                .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver Processing

# COMMAND ----------

# Read the data from the bronze schema
df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
display(df_bronze.limit(10))


# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC - **Drop Duplicates**

# COMMAND ----------

df_duplicates = df_bronze.groupBy("product_id").count().filter("count > 1")
display(df_duplicates)

# COMMAND ----------

print("Rows before duplicates dropped: ", df_bronze.count())
df_silver = df_bronze.dropDuplicates(['product_id'])
print("Rows after duplicates dropped: ", df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - **Title case fix** 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC (energy bars --> Energy Bars ...)

# COMMAND ----------

df_silver.select('category').distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "category", 
    F.when(
        F.col("category").isNull(), None)
        .otherwise(F.initcap("category"))
)
df_silver.select('category').distinct().show()

# COMMAND ----------

# Checking if any column got leading spaces
display(
    df_silver.filter(F.col("category") != F.trim("category"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC - **Fix Spelling Mistak for Protien**

# COMMAND ----------

# Replace 'protien' -> 'protein' in both product_name and category

df_silver = (
  df_silver.withColumn(
    "product_name",
    F.regexp_replace(F.col("product_name"), "(?i)Protien", "Protein")
  )
  .withColumn(
    "category",
    F.regexp_replace(F.col("category"), "(?i)Protien", "Protein")
  )
)

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC - **Standardizing Customer Attributes to Match Parent Company Data Model**

# COMMAND ----------

# Add division column
df_silver = df_silver.withColumn(
    "division",
    F.when(F.col("category") =="Energy Bar", "Nutrition Bars")
    .when(F.col("category") =="Protein Bar", "Nutrition Bars")
    .when(F.col("category") =="Granola & Cereals", "Breakfast Foods")
    .when(F.col("category") =="Recovery Dairy", "Diary & Recovery")
    .when(F.col("category") =="Healthy Snacks", "Healthy Snacks")
    .when(F.col("category") =="Electrolyte Mix", "Hydration & Electrolytes")
    .otherwise("other")
)

# Variant column
df_silver = df_silver.withColumn(
    "variant",
    F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)
)

# Create new column: product_code

# Invalid product_ids are replaced with a fallback value to avoid losing fact records and ensure downstream joins remain consistent

df_silver = (
    df_silver
    # 1. Generate deterministic product_code from product_name
    .withColumn(
        "product_code",
        F.sha2(F.col("product_name").cast("string"), 256)
    )
    # 2. Clean product_id: keep only numeric IDs, else set to 999999
    .withColumn(
        "product_id",
        F.when(
            F.col("product_id").cast("string").rlike("^[0-9]+$"),
            F.col("product_id").cast("string")
        ).otherwise(F.lit(999999).cast("string"))
    )
    # 3. Rename product_name -> product
    .withColumnRenamed("product_name", "product")
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.select("product_code", "division", "category", "product", "variant", "product_id", "read_timestamp", "file_name", "file_size")
display(df_silver)

# COMMAND ----------

df_silver.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed","true")\
            .option("mergeSchema", "true")\
                .mode("overwrite")\
                    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source}")
df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")
display(df_gold.limit(5))

# COMMAND ----------

df_gold.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .mode("overwrite")\
                .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC - **Merging Data source with Parent**

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_products")
df_child_products = spark.sql(f"SELECT product_code, division, category, product, variant FROM fmcg.gold.sb_dim_products;")
df_child_products.show(5)

# COMMAND ----------

delta_table.alias("target").merge(
    source = df_child_products.alias("source"),
    condition = "target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values = {
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).execute()