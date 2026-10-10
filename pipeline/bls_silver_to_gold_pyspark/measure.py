from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="measure")
def create_gold_measure_table():

    return (
        spark.read.table("workspace.silver.silver_pr_measure")
        .select(
            "measure_code",
            "measure_text",
        )
        .dropDuplicates([
            "measure_code",
        ])
        .withColumn( "inserted_datetime", F.current_timestamp(), )
    )