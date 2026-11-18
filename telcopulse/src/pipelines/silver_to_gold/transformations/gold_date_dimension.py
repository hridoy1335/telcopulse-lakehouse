"""Generate a reusable calendar dimension inside the Gold Lakeflow pipeline."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.gold_config import GOLD_TABLE_PROPERTIES


@dp.materialized_view(
    name="dim_date",
    comment="Gold calendar dimension generated for 2000-01-01 through 2040-12-31",
    table_properties={
        **GOLD_TABLE_PROPERTIES,
        "model": "dimension",
        "scd.type": "0",
    },
    cluster_by=["date_key"],
)
def dim_date():
    calendar = spark.sql(
        """
        SELECT explode(
          sequence(
            DATE '2000-01-01',
            DATE '2050-12-31',
            INTERVAL 1 DAY
          )
        ) AS calendar_date
        """
    )

    return (
        calendar
        .withColumn(
            "date_key",
            F.date_format("calendar_date", "yyyyMMdd").cast("int"),
        )
        .withColumn("day_of_month", F.dayofmonth("calendar_date"))
        .withColumn("day_of_week", F.dayofweek("calendar_date"))
        .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("calendar_date"))
        .withColumn("month_number", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("quarter_number", F.quarter("calendar_date"))
        .withColumn("year_number", F.year("calendar_date"))
        .withColumn(
            "is_weekend",
            F.dayofweek("calendar_date").isin(1, 7),
        )
        .withColumn(
            "year_month",
            F.date_format("calendar_date", "yyyy-MM"),
        )
    )