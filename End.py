from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assume df_fastbusiness has effective_date
# Assume df_businessstates has business_date (pre-loaded list of business days)

# Add row_id to uniquely identify each row
df_fastbusiness = df_fastbusiness.withColumn(
    "row_id", F.row_number().over(Window.orderBy("effective_date"))
)

# Step 1: Cross-join effective dates with business dates where business_date >= effective_date
df_cross = df_fastbusiness.alias("eff").join(
    df_businessstates.alias("biz"),
    F.col("biz.business_date") >= F.col("eff.effective_date"),
    how="inner"
)

# Step 2: For each effective_date, get the MIN business_date >= it (i.e. first one)
window = Window.partitionBy("eff.row_id").orderBy("biz.business_date")

df_with_rank = df_cross.withColumn("rank", F.row_number().over(window)) \
    .filter(F.col("rank") == 1) \
    .select(
        F.col("eff.effective_date"),
        F.col("biz.business_date").alias("next_business_day")
    )
