from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, FloatType

# ── Exchange rates to USD (as of April 2026, hardcoded for reproducibility) ──
EXCHANGE_RATES = {
    "AT": 1.08,   "AU": 0.65,   "BG": 0.60, "CA": 0.74,   
    "CH": 1.12,   "CL": 0.001,  "CN": 0.138,  "CZ": 0.044,
    "DE": 1.08,   "DK": 0.145,  "EG": 0.021,  "ES": 1.08,
    "FI": 1.08,   "FR": 1.08,   "GB": 1.27,   "GR": 1.08,
    "HR": 0.155,  "HU": 1.0,    "ID": 0.000062,"IE": 1.08,
    "IL": 0.27,   "IN": 0.012,  "IT": 1.08,   "JP": 0.0067,
    "KR": 0.00068,"LU": 1.08,   "MX": 0.052,  "MY": 0.226,  
    "NL": 1.08,   "NO": 0.094,  "NZ": 0.60,   "PH": 0.0174, 
    "PL": 0.25,   "PT": 1.08,   "RO": 0.217,  "SE": 0.096,
    "SG": 0.745,  "SI": 1.08,   "SK": 1.08,   "TH": 0.028,  
    "TR": 0.029,  "TW": 0.031,  "US": 1.0,    "VN": 0.000038, 
    "ZA": 0.054,
}


def main():
    spark = SparkSession.builder \
        .appName("NikePipelineTransform") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    RAW_PATH = "gs://nike-pipeline-raw/Nike data/*.csv"
    OUTPUT_PATH = "gs://nike-pipeline-processed/cleaned/"

    # Explicit schema — avoids inferSchema mistyping price_local as STRING
    # and ensures consistent types across all 45 country files.
    # mode=DROPMALFORMED discards rows where unescaped quotes
    # in product names shift all column values out of alignment.
    schema = StructType([
        StructField("snapshot_date",         StringType(), True),
        StructField("country_code",          StringType(), True),
        StructField("product_name",          StringType(), True),
        StructField("model_number",          StringType(), True),
        StructField("currency",              StringType(), True),
        StructField("price_local",           FloatType(),  True),
        StructField("sale_price_local",      FloatType(),  True),
        StructField("gender_segment",        StringType(), True),
        StructField("size_label",            StringType(), True),
        StructField("category",              StringType(), True),
        StructField("subcategory",           StringType(), True),
        StructField("product_id",            StringType(), True),
        StructField("sku",                   StringType(), True),
        StructField("style_color",           StringType(), True),
        StructField("brand_name",            StringType(), True),
        StructField("color_name",            StringType(), True),
        StructField("size_count",            StringType(), True),
        StructField("available_size_count",  StringType(), True),
        StructField("available",             StringType(), True),
        StructField("availability_level",    StringType(), True),
        StructField("available_market",      StringType(), True),
        StructField("in_stock",              StringType(), True),
        StructField("discount_pct",          FloatType(),  True),
        StructField("employee_price",        StringType(), True),
        StructField("product_url",           StringType(), True),
        StructField("canonical_url",         StringType(), True),
        StructField("image_url",             StringType(), True),
        StructField("gtin",                  StringType(), True),
        StructField("stock_keeping_unit_id", FloatType(),  True),
        StructField("catalog_sku_id",        StringType(), True),
        StructField("nike_size",             StringType(), True),
        StructField("localized_size",        StringType(), True),
        StructField("size_conversion_id",    StringType(), True),
        StructField("sport_tags",            StringType(), True),
        StructField("record_source",         StringType(), True),
    ])

    print(">>> Reading raw CSVs from GCS...")
    df = spark.read \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .schema(schema) \
        .csv(RAW_PATH)

    print(f">>> Raw row count: {df.count()}")

    # ── 1. Drop rows missing critical fields ──────────────────────────────────
    df = df.filter(
        F.col("product_name").isNotNull() &
        (F.trim(F.col("product_name")) != "") &
        F.col("price_local").isNotNull() &
        (F.col("price_local") < 100000)
    )

    # ── 2. Round fields with monetary values ──────────────────────────────────
    for monetary_col in ["price_local", "sale_price_local", "employee_price", "discount_pct"]:
        df = df.withColumn(
            monetary_col,
            F.round(F.col(monetary_col).cast(DoubleType()), 2)
        )

    # ── 3. Fill missing discount_pct with 0 and normalize to positive ────────
    # Raw data stores discounts as negative values (e.g. -17.35 means 17.35% off)
    # normalize to positive for intuitive downstream use in BigQuery and Looker
    df = df.withColumn(
        "discount_pct",
        F.when(F.col("discount_pct").isNull(), F.lit(0.0))
         .otherwise(F.abs(F.col("discount_pct").cast(DoubleType())))
    )

    # ── 4. Standardize string columns ────────────────────────────────────────
    string_cols = ["product_name", "category", "gender_segment", "country_code"]
    for col in string_cols:
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))

    # Lowercase categoricals for consistent grouping
    for col in ["category", "gender_segment"]:
        if col in df.columns:
            df = df.withColumn(col, F.lower(F.col(col)))

    # ── 5. Currency normalization to USD ─────────────────────────────────────
    # Build a Spark map literal from the exchange rate dict
    rate_map = F.create_map(*[
        item for pair in [(F.lit(k), F.lit(v)) for k, v in EXCHANGE_RATES.items()]
        for item in pair
    ])

    # Use country_code to look up the rate; fall back to 1.0 if not found
    df = df.withColumn(
        "exchange_rate_to_usd",
        F.coalesce(rate_map[F.col("country_code")], F.lit(1.0))
    )

    df = df.withColumn(
        "price_usd",
        F.round(
            F.col("price_local").cast(DoubleType()) * F.col("exchange_rate_to_usd"),
            2
        )
    )

    # ── 6. Derived helper columns ─────────────────────────────────────────────
    # Flag discounted products — discount_pct is now normalized to positive,
    # so any value > 0 means a discount is applied
    df = df.withColumn(
        "is_discounted",
        F.when(F.col("discount_pct") > 0, 1).otherwise(0)
    )

    # Sale price in USD — discount_pct is now positive (e.g. 17.35 = 17.35% off)
    df = df.withColumn(
        "sale_price_usd",
        F.round(
            F.col("price_usd") * (1 - F.col("discount_pct") / 100),
            2
        )
    )

    # ── 7. Drop helper column not needed downstream ───────────────────────────
    df = df.drop("exchange_rate_to_usd",
                 "size_count", "available_size_count", 
                 "gtin", "employee_price", "record_source")

    # ── 8. Final schema check ─────────────────────────────────────────────────
    print(">>> Final schema:")
    df.printSchema()
    print(f">>> Clean row count: {df.count()}")
    print(">>> Sample rows:")
    df.select(
        "country_code", "product_name", "category",
        "gender_segment", "price_local", "price_usd",
        "discount_pct", "sale_price_usd", "is_discounted"
    ).show(5, truncate=40)

    # ── 9. Write to GCS as Parquet ────────────────────────────────────────────
    print(f">>> Writing Parquet to {OUTPUT_PATH}...")
    df.write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH)

    print(">>> Transform complete.")
    spark.stop()


if __name__ == "__main__":
    main()