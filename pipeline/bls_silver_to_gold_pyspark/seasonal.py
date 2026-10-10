from pyspark import pipelines as dp


@dp.table(name="seasonal")
def create_gold_seasonal_table():

    return (
        spark.read.table("workspace.silver.silver_pr_seasonal")
        .select(
            "seasonal_code",
            "seasonal_text",
        )
        .dropDuplicates([
            "seasonal_code",
        ])
        .withColumn( "inserted_datetime", F.current_timestamp(), )
    )