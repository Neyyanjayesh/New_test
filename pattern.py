from pyspark.sql.functions import when, regexp_extract, lower, col

df = df.withColumn("recurrence_text_lower", lower(col("recurrence_text")))

df = df.withColumn("pattern_type",
    when(col("frequency") == "Daily", 
         when(col("recurrence_text_lower").contains("every business day"), "business_day")
        .when(col("recurrence_text_lower").contains("first business day"), "business_day")
        .when(col("recurrence_text_lower").contains("last business day"), "business_day")
        .when(col("recurrence_text_lower").contains("every day"), "daily")
    ).when(col("frequency") == "Annually", "annual")
    .when(col("frequency") == "Every 6 Months", "half_yearly")
    .when(col("frequency") == "Every other Week", "bi_weekly")
    .when(col("frequency") == "Monthly", 
         when(col("recurrence_text_lower").contains("first business day"), "business_day")
        .when(col("recurrence_text_lower").contains("last business day"), "business_day")
        .when(col("recurrence_text_lower").contains("first of"), "monthly")
        .otherwise("monthly")
    ).when(col("frequency") == "Quarterly", "quarterly")
    .when(col("frequency") == "One-Time", "one_time")
)

df = df.withColumn("pattern_value",
    when(col("pattern_type") == "business_day",
         when(col("recurrence_text_lower").contains("every business day"), "every")
        .when(col("recurrence_text_lower").contains("first business day"), "first")
        .when(col("recurrence_text_lower").contains("last business day"), "last")
    ).when(col("pattern_type") == "daily", "every")
    .when(col("pattern_type") == "annual", regexp_extract(col("recurrence_text_lower"), r"day (\d+)", 1))
    .when(col("pattern_type") == "half_yearly", regexp_extract(col("recurrence_text_lower"), r"day (\d+)", 1))
    .when(col("pattern_type") == "bi_weekly", regexp_extract(col("recurrence_text_lower"), r"on (\w+)", 1))
    .when((col("pattern_type") == "monthly") | (col("pattern_type") == "quarterly"),
          regexp_extract(col("recurrence_text_lower"), r"day (\d+)", 1))
    .when((col("pattern_type") == "monthly") & (col("recurrence_text_lower").contains("first of")), "1")
)

# Drop helper column
df = df.drop("recurrence_text_lower")

df.select("frequency", "recurrence_text", "pattern_type", "pattern_value").show(truncate=False)
