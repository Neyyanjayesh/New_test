from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# First extract all components using regexp_extract
df = df.withColumn("day", F.regexp_extract(F.lower(F.col("Recurrence Text")), r"day (\d+) of every", 1).cast("int"))
       .withColumn("eff_day", F.regexp_extract(F.lower(F.col("Recurrence Text")), r"effective (\d{2})-([a-z]{3})-(\d{2})", 1).cast("int"))
       .withColumn("eff_month", F.regexp_extract(F.lower(F.col("Recurrence Text")), r"effective (\d{2})-([a-z]{3})-(\d{2})", 2))
       .withColumn("eff_year", (F.regexp_extract(F.lower(F.col("Recurrence Text")), r"effective (\d{2})-([a-z]{3})-(\d{2})", 3).cast("int") + 2000))

# Convert month abbreviations to numbers (using a mapping)
month_map = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}
mapping_expr = F.create_map([F.lit(x) for x in sum(month_map.items(), ())])

df = df.withColumn("eff_month_num", mapping_expr[F.col("eff_month")])

# Create effective date column
df = df.withColumn("effective_date", F.to_date(
    F.concat_ws("-", F.col("eff_year"), F.col("eff_month_num"), F.col("eff_day"))
))

# Calculate last day of month
df = df.withColumn("last_day", F.last_day(F.col("effective_date")))

# Calculate derived day (min of target day and last day)
df = df.withColumn("derived_day", F.least(F.col("day"), F.dayofmonth(F.col("last_day"))))

# Create derived date in same month
df = df.withColumn("derived_date_same_month", F.to_date(
    F.concat_ws("-", F.col("eff_year"), F.col("eff_month_num"), F.col("derived_day"))
))

# Determine if we need to move to next month
df = df.withColumn("needs_next_month", 
    F.col("derived_date_same_month") < F.col("effective_date")
)

# Calculate next month's date
df = df.withColumn("next_month_date", 
    F.when(F.col("needs_next_month"),
        F.to_date(
            F.concat_ws("-",
                F.when(F.col("eff_month_num") == 12, F.col("eff_year") + 1).otherwise(F.col("eff_year")),
                F.when(F.col("eff_month_num") == 12, 1).otherwise(F.col("eff_month_num") + 1),
                F.col("derived_day")
            )
        )
    ).otherwise(F.col("derived_date_same_month"))
)

# Handle case where derived_day might exceed next month's days
df = df.withColumn("next_month_last_day", 
    F.last_day(F.col("next_month_date"))
)
df = df.withColumn("final_derived_day",
    F.least(F.col("derived_day"), F.dayofmonth(F.col("next_month_last_day")))
)

df = df.withColumn("Derived Date", 
    F.date_format(
        F.to_date(
            F.concat_ws("-",
                F.year(F.col("next_month_date")),
                F.month(F.col("next_month_date")),
                F.col("final_derived_day")
            )
        ),
        "dd-MM-yyyy"
    )
)

# Select only the columns we need
df = df.select("Recurrence Text", "Derived Date")
df.show(truncate=False)
