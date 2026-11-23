# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce Analytics Pipeline
# MAGIC Raw CSV -> Bronze (Delta) -> Silver (Delta) -> Gold (Delta) -> BigQuery -> Looker Studio
# MAGIC
# MAGIC Import this file into Databricks (Workspace > Import). Run cells top to bottom.
# MAGIC Sections: 00 Config | 01 Bronze | 02 Silver | 03 Gold | 04 BigQuery | 05 Tests + resume metrics

# COMMAND ----------
# MAGIC %md ## 00 Config

# COMMAND ----------

RAW_PATH    = "/Volumes/workspace/sales/sales_data/sample_data/"
BRONZE_PATH = "/Volumes/workspace/sales/sales_data/bronze/"
SILVER_PATH = "/Volumes/workspace/sales/sales_data/silver/"
GOLD_PATH   = "/Volumes/workspace/sales/sales_data/gold/"

GCP_PROJECT       = "gen-lang-client-0000887766"
BQ_DATASET        = "ecommerce"
TEMP_GCS_BUCKET   = "ecommerce-temp-bucket"
GCP_SECRET_SCOPE  = "SCOPE_GCP"
GCP_SECRET_KEY    = "SECRET_KEY"   # base64-encoded service-account JSON

# Date formats tried in order when parsing string dates (first match wins)
DATE_FORMATS = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "dd-MM-yyyy", "MM/dd/yyyy"]

# COMMAND ----------
# MAGIC %md ## 01 Bronze: ingest raw CSVs with schema enforcement
# MAGIC Fixes vs. original: date columns kept as strings (so bad formats are not silently nulled),
# MAGIC `pproduct_id` typo fixed, `overwriteSchema` typo fixed, `transections` folder renamed to `transactions`,
# MAGIC lineage columns added.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType)

schemas = {
    "transactions": StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
    ]),
    "customers": StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("region", StringType(), True),
        StructField("signup_date", StringType(), True),
    ]),
    "feedback": StructType([
        StructField("feedback_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),      # was "pproduct_id"
        StructField("rating", IntegerType(), True),
        StructField("review", StringType(), True),
        StructField("date", StringType(), True),
    ]),
    "products": StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("brand", StringType(), True),
    ]),
    "promotions": StructType([
        StructField("promotion_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("discount", DoubleType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("channel", StringType(), True),
    ]),
    "stores": StructType([
        StructField("store_id", IntegerType(), True),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("manager", StringType(), True),
        StructField("opened_date", StringType(), True),
    ]),
}

bronze_counts = {}
for name, schema in schemas.items():
    df = (spark.read.option("header", True).option("mode", "PERMISSIVE")
          .schema(schema).csv(RAW_PATH + f"{name}.csv")
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingest_time", F.current_timestamp()))
    (df.write.mode("overwrite").format("delta")
       .option("overwriteSchema", True).save(BRONZE_PATH + name))
    bronze_counts[name] = df.count()
    print(f"bronze.{name}: {bronze_counts[name]:,} rows")

# COMMAND ----------
# MAGIC %md ## 02 Silver: clean, validate, enrich
# MAGIC Fixes vs. original:
# MAGIC 1. Feedback joined on `customer_id` only -> fan-out, duplicated transactions and inflated revenue. Now aggregated to (customer, product) first.
# MAGIC 2. Promotions joined on `product_id` only and the date-range check was computed *after* the discount was applied. Now the date window is part of the join, and the best discount is chosen.
# MAGIC 3. Null `total_amount` was silently set to 0. Now imputed as quantity x price and flagged.
# MAGIC 4. Bad rows go to a quarantine table instead of being dropped or hidden.
# MAGIC 5. Date parsing is done once, with explicit formats.

# COMMAND ----------

def parse_date(col_name):
    """Try each format; return NULL (not an error) if none match."""
    return F.coalesce(*[
        F.to_date(F.expr(f"try_to_timestamp(`{col_name}`, '{fmt}')")) for fmt in DATE_FORMATS
    ])

trn   = spark.read.format("delta").load(BRONZE_PATH + "transactions")
cust  = spark.read.format("delta").load(BRONZE_PATH + "customers")
prod  = spark.read.format("delta").load(BRONZE_PATH + "products")
fb    = spark.read.format("delta").load(BRONZE_PATH + "feedback")
store = spark.read.format("delta").load(BRONZE_PATH + "stores")
promo = spark.read.format("delta").load(BRONZE_PATH + "promotions")

# ---- dimensions: parse dates, dedupe on business key
cust  = cust.withColumn("signup_date", parse_date("signup_date")).dropDuplicates(["customer_id"])
prod  = prod.dropDuplicates(["product_id"])
store = store.withColumn("opened_date", parse_date("opened_date")).dropDuplicates(["store_id"])
promo = (promo.withColumn("start_date", parse_date("start_date"))
              .withColumn("end_date", parse_date("end_date"))
              .withColumn("discount_frac",                      # accept 20 or 0.20
                          F.when(F.col("discount") > 1, F.col("discount") / 100).otherwise(F.col("discount"))))
fb = (fb.withColumn("date", parse_date("date"))
        .filter(F.col("rating").between(1, 5)))

# ---- transactions: parse, quarantine bad rows, dedupe
trn_p = trn.withColumn("transaction_date", parse_date("transaction_date"))

bad_cond = (
    F.col("transaction_id").isNull()
    | F.col("transaction_date").isNull()
    | F.coalesce(F.col("quantity") <= 0, F.lit(True))
    | F.coalesce(F.col("total_amount") < 0, F.lit(False))
)
quarantine = trn_p.filter(bad_cond).withColumn(
    "_reason",
    F.when(F.col("transaction_id").isNull(), "null_transaction_id")
     .when(F.col("transaction_date").isNull(), "unparseable_date")
     .when(F.coalesce(F.col("quantity") <= 0, F.lit(True)), "invalid_quantity")
     .otherwise("negative_amount"))
quarantine.write.mode("overwrite").format("delta").option("overwriteSchema", True) \
          .save(SILVER_PATH + "transactions_quarantine")

trn_valid = trn_p.filter(~bad_cond)
rows_before_dedupe = trn_valid.count()
trn_clean = trn_valid.dropDuplicates(["transaction_id"])
rows_after_dedupe = trn_clean.count()

# ---- best active promo per transaction (date window in the join, avoids fan-out)
t = trn_clean.alias("t"); pr = promo.alias("pr")
promo_match = (
    t.join(pr,
           (F.col("t.product_id") == F.col("pr.product_id"))
           & F.col("t.transaction_date").between(F.col("pr.start_date"), F.col("pr.end_date")),
           "inner")
     .groupBy("t.transaction_id")
     .agg(F.max("pr.discount_frac").alias("discount_frac"),
          F.count("*").alias("promos_matched"))
)

# ---- feedback aggregated to one row per (customer, product)
fb_agg = (fb.groupBy("customer_id", "product_id")
            .agg(F.avg("rating").alias("avg_rating"), F.count("*").alias("review_count")))

# ---- assemble silver
silver = (
    trn_clean
    .join(promo_match, "transaction_id", "left")
    .join(cust.select("customer_id", "name", "age", "gender", "region", "signup_date"), "customer_id", "left")
    .join(prod.select("product_id", "product_name", "category", "brand", "price"), "product_id", "left")
    .join(store.select("store_id", "store_name", "location"), "store_id", "left")
    .join(fb_agg, ["customer_id", "product_id"], "left")
    .withColumn("amount_imputed", F.col("total_amount").isNull() & F.col("price").isNotNull())
    .withColumn("total_amount", F.coalesce(F.col("total_amount"), F.col("quantity") * F.col("price")))
    .withColumn("promo_applied", F.col("discount_frac").isNotNull())
    .withColumn("final_amount",
                F.round(F.col("total_amount") * (1 - F.coalesce(F.col("discount_frac"), F.lit(0.0))), 2))
    .withColumn("discount_amount", F.round(F.col("total_amount") - F.col("final_amount"), 2))
    .withColumn("is_valid_store", F.col("store_name").isNotNull())
    .withColumn("has_valid_customer", F.col("name").isNotNull())
    .withColumn("has_valid_product", F.col("product_name").isNotNull())
    .withColumn("txn_month", F.date_format("transaction_date", "yyyy-MM"))
    .withColumn("_processed_time", F.current_timestamp())
)

(silver.write.mode("overwrite").format("delta").option("overwriteSchema", True)
       .partitionBy("txn_month").save(SILVER_PATH + "transaction_enriched"))

silver = spark.read.format("delta").load(SILVER_PATH + "transaction_enriched")
print(f"silver rows: {silver.count():,} | quarantined: {quarantine.count():,} | "
      f"duplicates removed: {rows_before_dedupe - rows_after_dedupe:,}")

# COMMAND ----------
# MAGIC %md ## 03 Gold: business KPI tables

# COMMAND ----------

from pyspark.sql import Window

ok = silver.filter("has_valid_product")           # rows safe for product/category KPIs

gold = {}

gold["daily_sales"] = (silver.groupBy("transaction_date").agg(
    F.countDistinct("transaction_id").alias("orders"),
    F.sum("quantity").alias("units"),
    F.round(F.sum("total_amount"), 2).alias("gross_revenue"),
    F.round(F.sum("final_amount"), 2).alias("net_revenue"),
    F.round(F.sum("discount_amount"), 2).alias("discount_given"),
    F.round(F.avg("final_amount"), 2).alias("avg_order_value")).orderBy("transaction_date"))

gold["sales_by_category"] = (ok.groupBy("txn_month", "category").agg(
    F.sum("quantity").alias("units"),
    F.round(F.sum("final_amount"), 2).alias("net_revenue")))

gold["sales_by_store"] = (silver.filter("is_valid_store").groupBy("store_id", "store_name", "location").agg(
    F.countDistinct("transaction_id").alias("orders"),
    F.round(F.sum("final_amount"), 2).alias("net_revenue")))

gold["top_products"] = (ok.groupBy("product_id", "product_name", "category", "brand").agg(
    F.sum("quantity").alias("units"),
    F.round(F.sum("final_amount"), 2).alias("net_revenue"),
    F.round(F.avg("avg_rating"), 2).alias("avg_rating"))
    .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.col("net_revenue").desc())))
    .filter("revenue_rank <= 20"))

cust_agg = (silver.filter("has_valid_customer")
            .groupBy("customer_id", "name", "region", "age", "gender").agg(
    F.countDistinct("transaction_id").alias("orders"),
    F.round(F.sum("final_amount"), 2).alias("total_spend"),
    F.max("transaction_date").alias("last_purchase")))
gold["customer_segments"] = (cust_agg
    .withColumn("spend_pct", F.percent_rank().over(Window.orderBy(F.col("total_spend").desc())))
    .withColumn("segment",
                F.when(F.col("spend_pct") <= 0.10, "VIP")
                 .when(F.col("spend_pct") <= 0.40, "Loyal")
                 .when(F.col("spend_pct") <= 0.80, "Regular")
                 .otherwise("Occasional")))

gold["promo_effectiveness"] = (silver.groupBy("promo_applied").agg(
    F.countDistinct("transaction_id").alias("orders"),
    F.round(F.avg("quantity"), 2).alias("avg_units_per_order"),
    F.round(F.avg("final_amount"), 2).alias("avg_order_value"),
    F.round(F.sum("discount_amount"), 2).alias("discount_given"),
    F.round(F.sum("final_amount"), 2).alias("net_revenue")))

# Ratings come straight from feedback (only a few transactions match a review on customer+product,
# so deriving ratings via transactions would throw away almost all reviews).
gold["product_ratings"] = (fb.join(prod.select("product_id", "category"), "product_id", "inner")
    .groupBy("category").agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("reviews")))

for name, df in gold.items():
    df.write.mode("overwrite").format("delta").option("overwriteSchema", True).save(GOLD_PATH + name)
    print(f"gold.{name}: {df.count():,} rows")

# COMMAND ----------
# MAGIC %md ## 04 Load Gold into BigQuery (for Looker Studio)
# MAGIC `SECRET_KEY` must hold the **base64-encoded** service-account JSON. Never commit it or print it.

# COMMAND ----------

def to_bigquery(df, table):
    (df.write.format("bigquery")
       .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.{table}")
       .option("parentProject", GCP_PROJECT)
       .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
       .option("credentials", dbutils.secrets.get(GCP_SECRET_SCOPE, GCP_SECRET_KEY))
       .mode("overwrite").save())
    print(f"loaded {BQ_DATASET}.{table}")

for name in gold:
    to_bigquery(spark.read.format("delta").load(GOLD_PATH + name), name)

# COMMAND ----------
# MAGIC %md ## 05 Tests + metrics for your resume
# MAGIC All checks must PASS. The metrics printed at the end are the real numbers for your resume and LinkedIn post.

# COMMAND ----------

results = []
def check(name, condition):
    results.append((name, bool(condition)))
    print(("PASS  " if condition else "FAIL  ") + name)

silver = spark.read.format("delta").load(SILVER_PATH + "transaction_enriched")
quar   = spark.read.format("delta").load(SILVER_PATH + "transactions_quarantine")

n_silver = silver.count()
check("no duplicate transaction_id in silver", n_silver == silver.select("transaction_id").distinct().count())
check("no null keys (transaction_id, customer_id, product_id)",
      silver.filter("transaction_id is null or customer_id is null or product_id is null").count() == 0)
check("silver rows == deduped valid bronze rows (no join fan-out)", n_silver == rows_after_dedupe)
check("raw rows == silver + duplicates + quarantined",
      bronze_counts["transactions"] == n_silver + (rows_before_dedupe - rows_after_dedupe) + quar.count())
check("final_amount <= total_amount everywhere", silver.filter("final_amount > total_amount").count() == 0)
check("no negative amounts or non-positive quantities", silver.filter("final_amount < 0 or quantity <= 0").count() == 0)
check("no null transaction_date", silver.filter("transaction_date is null").count() == 0)
check("gold daily net revenue == silver net revenue",
      abs(gold["daily_sales"].agg(F.sum("net_revenue")).first()[0]
          - silver.agg(F.sum("final_amount")).first()[0]) < 1.0)
check("ratings within 1..5", fb.filter("rating < 1 or rating > 5").count() == 0)

failed = [n for n, ok_ in results if not ok_]
assert not failed, f"Failed checks: {failed}"

# COMMAND ----------

agg = silver.agg(
    F.sum("final_amount").alias("net_revenue"),
    F.sum("discount_amount").alias("discount_given"),
    F.min("transaction_date").alias("date_min"),
    F.max("transaction_date").alias("date_max"),
    F.countDistinct("customer_id").alias("customers"),
    F.countDistinct("product_id").alias("products"),
    F.countDistinct("store_id").alias("stores"),
    F.avg(F.col("promo_applied").cast("int")).alias("promo_share"),
    F.avg(F.col("is_valid_store").cast("int")).alias("valid_store_share"),
    F.avg(F.col("has_valid_customer").cast("int")).alias("valid_customer_share"),
    F.sum(F.col("amount_imputed").cast("int")).alias("amounts_imputed")).first()

METRICS = {
    "raw_rows_by_table": bronze_counts,
    "transactions_raw": bronze_counts["transactions"],
    "transactions_silver": n_silver,
    "quarantined_rows": quar.count(),
    "duplicates_removed": rows_before_dedupe - rows_after_dedupe,
    "missing_amounts_imputed": agg["amounts_imputed"],
    "tables_total": len(schemas) + 2 + len(gold),   # bronze + silver(2) + gold
    "gold_tables": len(gold),
    "dq_checks_passed": len(results),
    "date_range": f"{agg['date_min']} to {agg['date_max']}",
    "distinct_customers": agg["customers"],
    "distinct_products": agg["products"],
    "distinct_stores": agg["stores"],
    "net_revenue": round(agg["net_revenue"], 2),
    "discount_given": round(agg["discount_given"], 2),
    "promo_share_pct": round(100 * agg["promo_share"], 1),
    "valid_store_pct": round(100 * agg["valid_store_share"], 1),
    "valid_customer_pct": round(100 * agg["valid_customer_share"], 1),
}
import json
print(json.dumps(METRICS, indent=2, default=str))
