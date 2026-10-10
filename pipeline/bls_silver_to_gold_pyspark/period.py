from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="period")
def create_gold_period_table():

    return (
        spark.read.table("workspace.silver.silver_pr_period")
        .select(
            "period",
            "period_name",
        )
        .dropDuplicates([
            "period",
        ])
        .withColumn( "inserted_datetime", F.current_timestamp(), )
    )