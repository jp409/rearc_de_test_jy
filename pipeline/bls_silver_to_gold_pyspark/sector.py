from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="sector")
def create_gold_sector_table():

    return (
        spark.read.table("workspace.silver.silver_pr_sector")
        .select(
            "sector_code",
            "sector_name",
        )
        .dropDuplicates([
            "sector_code",
        ])
        .withColumn( "inserted_datetime", F.current_timestamp(), )
    )