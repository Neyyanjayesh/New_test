from pyspark.sql import functions as F
from pyspark.sql.window import Window

## Assuming df is your input DataFrame with "Recurrence Text" column
## Assuming df_business_dates contains all valid business dates

# First extract all possible components from the text
df = df.withColumn("pattern_type",
    F.when(F.col("Recurrence Text").rlike("(?i)every business day"), "daily_business")
     .when(F.col("Recurrence Text").rlike("(?i)every day"), "daily")
     .when(F.col("Recurrence Text").rlike("(?i)every \\d+ weeks"), "biweekly")
     .when(F.col("Recurrence Text").rlike("(?i)first business day"), "first_business")
     .when(F.col("Recurrence Text").rlike("(?i)last business day"), "last_business")
     .when(F.col("Recurrence Text").rlike("(?i)day \\d+ of every"), "monthly_fixed")
     .when(F.col("Recurrence Text").rlike("(?i)first of every"), "monthly_first")
     .otherwise("unknown")
)

# Extract common components
df = (df
    # Extract day number for monthly patterns
    .withColumn("day_num", 
        F.regexp_extract(F.lower(F.col("Recurrence Text")), r"day (\d+) of every", 1).cast("int"))
    
    # Extract effective date
    .withColumn("eff_date_str",
        F.regexp_extract(F.lower(F.col("Recurrence Text")), r"effective (\d{2}-[a-z]{3}-\d{2})", 1))
    .withColumn("effective_date",
        F.to_date(F.col("eff_date_str"), "dd-MMM-yy"))
    
    # Extract end date/occurrences if they exist
    .withColumn("end_condition",
        F.when(F.col("Recurrence Text").rlike("(?i)until \\d{2}-[a-z]{3}-\d{2}"),
            F.regexp_extract(F.lower(F.col("Recurrence Text")), r"until (\d{2}-[a-z]{3}-\d{2})", 1))
         .when(F.col("Recurrence Text").rlike("(?i)for \\d+ occurrences"),
            F.regexp_extract(F.col("Recurrence Text"), r"for (\d+) occurrences", 1))
         .otherwise(F.lit(None)))
)

# Handle monthly fixed day patterns (existing solution)
monthly_fixed = df.filter(F.col("pattern_type") == "monthly_fixed")
monthly_fixed = (monthly_fixed
    .withColumn("last_day", F.last_day(F.col("effective_date")))
    .withColumn("derived_day", F.least(F.col("day_num"), F.dayofmonth(F.col("last_day"))))
    .withColumn("derived_date", 
        F.to_date(F.concat_ws("-", 
            F.year(F.col("effective_date")),
            F.month(F.col("effective_date")),
            F.col("derived_day"))))
    .withColumn("needs_next_month", 
        F.col("derived_date") < F.col("effective_date"))
    # ... continue with your existing monthly logic ...
)

# Handle first business day of month
first_business = df.filter(F.col("pattern_type") == "first_business")
first_business = (first_business
    .join(df_business_dates, 
        (F.year(df_business_dates["business_date"]) == F.year(F.col("effective_date"))) &
        (F.month(df_business_dates["business_date"]) == F.month(F.col("effective_date"))))
    .groupBy("Recurrence Text", "effective_date")
    .agg(F.min("business_date").alias("derived_date"))
)

# Handle last business day of month
last_business = df.filter(F.col("pattern_type") == "last_business")
last_business = (last_business
    .join(df_business_dates, 
        (F.year(df_business_dates["business_date"]) == F.year(F.col("effective_date"))) &
        (F.month(df_business_dates["business_date"]) == F.month(F.col("effective_date"))))
    .groupBy("Recurrence Text", "effective_date")
    .agg(F.max("business_date").alias("derived_date"))
)

# Handle bi-weekly patterns
biweekly = df.filter(F.col("pattern_type") == "biweekly")
biweekly = (biweekly
    .withColumn("day_of_week",
        F.regexp_extract(F.lower(F.col("Recurrence Text")), r"on ([a-z]+)", 1))
    # Convert to day of week number (1=Sunday, 2=Monday, etc.)
    .withColumn("dow_num",
        F.when(F.col("day_of_week") == "sunday", 1)
         .when(F.col("day_of_week") == "monday", 2)
         .when(F.col("day_of_week") == "tuesday", 3)
         .when(F.col("day_of_week") == "wednesday", 4)
         .when(F.col("day_of_week") == "thursday", 5)
         .when(F.col("day_of_week") == "friday", 6)
         .when(F.col("day_of_week") == "saturday", 7))
    # Generate dates starting from effective_date
    .withColumn("date_series",
        F.expr("sequence(effective_date, date_add(effective_date, 365), interval 1 day)"))
    .withColumn("date", F.explode("date_series"))
    .filter(F.dayofweek("date") == F.col("dow_num"))
    .withColumn("week_diff",
        F.floor(F.datediff("date", "effective_date") / 14))
    .filter(F.col("week_diff") % 2 == 0)  # Every 2 weeks
    .groupBy("Recurrence Text", "effective_date")
    .agg(F.min("date").alias("derived_date"))  # First occurrence
)

# Handle every business day
daily_business = df.filter(F.col("pattern_type") == "daily_business")
daily_business = (daily_business
    .join(df_business_dates,
        df_business_dates["business_date"] >= F.col("effective_date"))
    .groupBy("Recurrence Text", "effective_date")
    .agg(F.min("business_date").alias("derived_date"))  # First business day after effective date
)

# Handle every day
daily = df.filter(F.col("pattern_type") == "daily")
daily = daily.withColumn("derived_date", F.col("effective_date"))

# Union all results
final_df = monthly_fixed.unionByName(first_business).unionByName(last_business) \
    .unionByName(biweekly).unionByName(daily_business).unionByName(daily)

# Format the final date
final_df = final_df.withColumn("Derived Date", 
    F.date_format(F.col("derived_date"), "dd-MM-yyyy"))
