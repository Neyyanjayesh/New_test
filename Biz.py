from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Sample df_business: contains all business dates
# Let's assume this DataFrame exists and has one column: business_date
# Sample df_effective: contains effective dates
# Columns: id, effective_date

# Step 1: Get first business date per month
df_first_business = df_business \
    .withColumn("year", F.year("business_date")) \
    .withColumn("month", F.month("business_date")) \
    .withColumn("rank", F.row_number().over(Window.partitionBy("year", "month").orderBy("business_date"))) \
    .filter(F.col("rank") == 1) \
    .select(F.col("business_date").alias("first_business_date"), "year", "month")

# Step 2: Add year and month columns to df_effective
df_effective_extended = df_effective \
    .withColumn("year", F.year("effective_date")) \
    .withColumn("month", F.month("effective_date"))

# Step 3: Join to get first business day of same month
df_joined = df_effective_extended.join(
    df_first_business,
    on=["year", "month"],
    how="left"
)

# Step 4: If effective_date <= first business day, then use that first business day.
# Else, get first business day of *next* month.
df_result = df_joined.withColumn(
    "derived_date",
    F.when(
        F.col("effective_date") <= F.col("first_business_date"),
        F.col("first_business_date")
    ).otherwise(None)  # We'll handle this next
)

# Step 5: For those where we need next month, get next month's first business day
# First, generate next_month, next_year
df_next = df_effective_extended \
    .withColumn("next_month", ((F.col("month") % 12) + 1)) \
    .withColumn("next_year", F.when(F.col("month") == 12, F.col("year") + 1).otherwise(F.col("year")))

# Join with df_first_business again
df_next_joined = df_next.join(
    df_first_business,
    (df_first_business["year"] == F.col("next_year")) & 
    (df_first_business["month"] == F.col("next_month")),
    how="left"
).select(
    df_next["id"],
    df_next["effective_date"],
    df_first_business["first_business_date"].alias("next_first_business_date")
)

# Step 6: Combine both cases
# Left join df_result with df_next_joined to get `next_first_business_date` where needed
df_final = df_result.join(df_next_joined, on=["id", "effective_date"], how="left") \
    .withColumn(
        "final_derived_date",
        F.coalesce("derived_date", "next_first_business_date")
    ).select("id", "effective_date", "final_derived_date")
