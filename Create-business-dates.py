from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from datetime import datetime, timedelta

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Define date range: today to one year from today
start_date = datetime.today().date()
end_date = start_date + timedelta(days=365)

# Generate list of business dates (Mon-Fri)
date_list = []
current_date = start_date
while current_date <= end_date:
    if current_date.weekday() < 5:  # 0 = Monday, ..., 4 = Friday
        date_list.append((current_date,))
    current_date += timedelta(days=1)

# Create DataFrame
DF_business_dates = spark.createDataFrame(date_list, ['trade_date']) \
                         .withColumn("trade_date", DF_business_dates["trade_date"].cast(DateType()))

# Show few rows
DF_business_dates.show(5)
