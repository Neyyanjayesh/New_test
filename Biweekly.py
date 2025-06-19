from pyspark.sql import functions as F

# Sample input
data = [
    ("2022-11-29", "Occurs every two weeks on Thursday effective 29 November 2022"),
    ("2022-12-01", "Occurs every two weeks on Monday effective 01 December 2022")
]

df = spark.createDataFrame(data, ["start_date", "recurrence_text"]) \
    .withColumn("start_date", F.to_date("start_date"))

# Day of week mapping for Spark (Monday = 1, ..., Sunday = 7)
day_of_week_map = {
    "monday": 1, "tuesday": 2, "wednesday": 3,
    "thursday": 4, "friday": 5, "saturday": 6, "sunday": 7
}

# Step 1: Extract day name from recurrence_text using regex
df = df.withColumn("weekday_text",
    F.regexp_extract(F.col("recurrence_text"), r'on (\w+)', 1)
)

# Step 2: Convert day name to number (using CASE WHEN)
df = df.withColumn("target_dow",
    F.when(F.lower(F.col("weekday_text")) == "monday", 1)
     .when(F.lower(F.col("weekday_text")) == "tuesday", 2)
     .when(F.lower(F.col("weekday_text")) == "wednesday", 3)
     .when(F.lower(F.col("weekday_text")) == "thursday", 4)
     .when(F.lower(F.col("weekday_text")) == "friday", 5)
     .when(F.lower(F.col("weekday_text")) == "saturday", 6)
     .when(F.lower(F.col("weekday_text")) == "sunday", 7)
)

# Step 3: Add 14 days
df = df.withColumn("base_date", F.expr("date_add(start_date, 14)"))

# Step 4: Get day of week of base_date
df = df.withColumn("base_dow", F.date_format(F.col("base_date"), 'u').cast("int"))

# Step 5: Calculate days until next target weekday
df = df.withColumn("days_ahead",
    F.when((F.col("target_dow") - F.col("base_dow")) <= 0,
           F.col("target_dow") - F.col("base_dow") + 7)
     .otherwise(F.col("target_dow") - F.col("base_dow"))
)

# Step 6: Add days_ahead to base_date to get next occurrence
df = df.withColumn("next_occurrence_date",
    F.expr("date_add(base_date, days_ahead)")
)

# Final result
df.select("start_date", "recurrence_text", "next_occurrence_date").show(truncate=False)
