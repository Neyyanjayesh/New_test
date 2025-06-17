from pyspark.sql import functions as F

# [Previous extraction and setup code remains the same...]

# Calculate next month's date with proper day adjustment
df = df.withColumn("next_month_year", 
    F.when(F.col("eff_month_num") == 12, F.col("eff_year") + 1)
       .otherwise(F.col("eff_year"))
)

df = df.withColumn("next_month_num", 
    F.when(F.col("eff_month_num") == 12, 1)
       .otherwise(F.col("eff_month_num") + 1)
)

# Calculate last day of next month
df = df.withColumn("next_month_last_day", 
    F.last_day(F.to_date(F.concat_ws("-", F.col("next_month_year"), F.col("next_month_num"), F.lit(1))))
)

# Calculate valid derived day for next month
df = df.withColumn("next_month_derived_day", 
    F.least(F.col("day"), F.dayofmonth(F.col("next_month_last_day")))
)

# Create the final derived date
df = df.withColumn("Derived Date", 
    F.when(F.col("needs_next_month"),
        F.date_format(
            F.to_date(F.concat_ws("-", 
                F.col("next_month_year"),
                F.col("next_month_num"),
                F.col("next_month_derived_day")
            )),
            "dd-MM-yyyy"
        )
    ).otherwise(
        F.date_format(F.col("derived_date_same_month"), "dd-MM-yyyy")
    )
)

# Handle null cases (invalid patterns)
df = df.withColumn("Derived Date", 
    F.when(
        F.col("day").isNull() | 
        F.col("eff_day").isNull() |
        F.col("eff_month_num").isNull() |
        F.col("eff_year").isNull(),
        F.lit(None)
    ).otherwise(F.col("Derived Date"))
)
