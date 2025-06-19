# df_business_dates
df_business_dates = spark.createDataFrame([
    ("2024-06-19",),
    ("2024-06-20",),
    ("2024-06-24",),
], ["trade_date"]).withColumn("trade_date", F.to_date("trade_date"))

# df_daily
df_daily = spark.createDataFrame([
    ("Occurs daily", "2024-06-19"),
    ("Occurs daily", "2024-06-22"),  # Saturday (not in business dates)
    ("Occurs daily", "2024-06-25"),  # No match, will be dropped
], ["recurrence_text", "start_date"]).withColumn("start_date", F.to_date("start_date"))



from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 0: Add unique row ID to df_daily
df_daily = df_daily.withColumn(
    "row_id", F.row_number().over(Window.orderBy("start_date"))
)

# Step 1: Join df_daily with df_business_dates where trade_date >= start_date
df_cross = df_daily.alias("daily").join(
    df_business_dates.alias("biz"),
    F.col("biz.trade_date") >= F.col("daily.start_date"),
    how="inner"
)

# Step 2: Get the first matching trade_date for each start_date
window = Window.partitionBy("daily.row_id").orderBy("biz.trade_date")

df_with_derived = df_cross.withColumn("rank", F.row_number().over(window)) \
    .filter(F.col("rank") == 1) \
    .select(
        "daily.start_date",
        F.col("biz.trade_date").alias("derived_date")
    )
