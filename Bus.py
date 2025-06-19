from pyspark.sql import functions as F

# Sample input
data = [
    ("2025-06-05", "Occurs every two weeks on Monday effective 5 June 2025"),  # Thursday start
    ("2025-06-10", "Occurs every two weeks on Friday effective 10 June 2025"),  # Tuesday start
]

df = spark.createDataFrame(data, ["start_date", "recurrence_text"]) \
    .withColumn("start_date", F.to_date("start_date"))

# Extract weekday text from recurrence_text
df = df.withColumn("weekday_text",
    F.regexp_extract("recurrence_text", r'on (\w+)', 1)
)

# Map weekday to Spark's dayofweek() values (Sunday = 1)
df = df.withColumn("target_dow",
    F.when(F.lower("weekday_text") == "sunday", 1)
     .when(F.lower("weekday_text") == "monday", 2)
     .when(F.lower("weekday_text") == "tuesday", 3)
     .when(F.lower("weekday_text") == "wednesday", 4)
     .when(F.lower("weekday_text") == "thursday", 5)
     .when(F.lower("weekday_text") == "friday", 6)
     .when(F.lower("weekday_text") == "saturday", 7)
)

# Get day of week for start_date (Sunday = 1)
df = df.withColumn("start_dow", F.dayofweek("start_date"))

# Step 1: Find first matching weekday ON or AFTER start_date
df = df.withColumn("days_to_first_occurrence",
    F.when((F.col("target_dow") - F.col("start_dow")) >= 0,
           F.col("target_dow") - F.col("start_dow"))
     .otherwise(F.col("target_dow") - F.col("start_dow") + 7)
)

df = df.withColumn("first_occurrence",
    F.expr("date_add(start_date, days_to_first_occurrence)")
)

# Step 2: Add 14 days to that to get the next recurrence
df = df.withColumn("next_occurrence_date",
    F.expr("date_add(first_occurrence, 14)")
)

# Final output
df.select("start_date", "recurrence_text", "next_occurrence_date").show(truncate=False)
