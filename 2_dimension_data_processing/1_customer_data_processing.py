# Databricks notebook source
# MAGIC %md
# MAGIC ###Bronze Processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/sandurash19@gmail.com/FMCG_Project/FMCG_Pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-sr123/{data_source}/*.csv'

print(base_path)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
    )
display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .mode("overwrite")\
                .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
df_bronze.show(10)
#display(df_bronze.limit(10))

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# Since you already got the table into a spark data frame, you can use that data frame to see if there's any duplicates.
'''
df_duplicates = spark.sql(
    f"""
    SELECT customer_id, COUNT(*) as count FROM {catalog}.{bronze_schema}.{data_source}
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    """
)

display(df_duplicates)
'''

# COMMAND ----------

# To check if we got any duplicate customer_id
df_duplicates = df_bronze.groupBy("customer_id").count().where("count > 1")
display(df_duplicates)

# COMMAND ----------

# Drop duplicate customer_ids
print("Rows before duplicates dropped: ", df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print("Rows after duplicates dropped: ", df_silver.count())


# COMMAND ----------

# To check if we got any unnecessary spaces in customer_name column
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

# Trim the spaces
df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

# Find all the distinct customer names
display(df_silver.select("customer_name").distinct())


# COMMAND ----------

# Title case fix
df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
    .otherwise(F.initcap("customer_name"))
)

display(df_silver.select("customer_name").distinct())

# COMMAND ----------

# Find all the distinct cities
display(df_silver.select('city').distinct())

# COMMAND ----------

# Typos -> correct names
city_mapping = {
    'Bengaluruu' : 'Bengaluru',
    'Bengalore' : 'Bengaluru',

    'Hyderbad' : 'Hyderabad',
    'Hyderabadd' : 'Hyderabad',

    'NewDelhee' : 'New Delhi',
    'NewDelhi' : 'New Delhi',
    'NewDheli' : 'New Delhi'
}

allowed = ['Bengaluru', 'Hyderabad', 'New Delhi']

df_silver = (
    df_silver
    .replace(city_mapping, subset=['city'])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
        .when(F.col("city").isin(allowed), F.col("city"))
        .otherwise(None)
    )
)


# COMMAND ----------

# Sanity check
display(df_silver.select("city").distinct())

# COMMAND ----------

# Cities with null

display(df_silver.filter(F.col("city").isNull()))
#df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']

display(df_silver.filter(F.col('customer_name').isin(null_customer_names)))

# COMMAND ----------

# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
    # Springtx Nutrition
    789403 : "New Delhi",

    # Zenathlete Foods
    789420 : "Bengaluru",

    # Primefuel Nutrition
    789521 : "Hyderabad",

    # Recovery Lane
    789603 : "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city") # Replace null with fixed city
    )
)

display(df_silver)

# COMMAND ----------

df_silver = df_silver.drop("fixed_city")

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# Converting customer_id to string
df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
df_silver.printSchema()

# COMMAND ----------

# Updating (& adding) columns' names in silver table to match the columns in parent gold table.

df_silver = (
    df_silver
    # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unkown")))
    )

    # Static attribbutes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

display(df_silver.limit(10))



# COMMAND ----------

df_silver = df_silver.withColumnRenamed("customer_id", "customer_code")
df_silver.printSchema()

# COMMAND ----------

# Save df_silver into a table in Silver Schema
df_silver.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
        .option("mergeSchema", "true")\
            .mode("overwrite")\
                .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

# Take only required columns
df_gold = df_silver.select("customer_code", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

# Writing the df_gold table into Gold schema
df_gold.write\
    .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
            .mode("overwrite")\
                .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customer = spark.table("fmcg.gold.sb_dim_customers").select(
    "customer_code",
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

# Upsert Operation
delta_table.alias("target").merge(
    source = df_child_customer.alias("source"),
    condition = "target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()