from pyspark import pipelines as dp


@dp.table(name="duration")
def create_gold_duration_table():

    return (
        spark.read.table("workspace.silver.silver_pr_duration")
        .select(
            "duration_code",
            "duration_text",
        )
        .dropDuplicates([
            "duration_code",
        ])
        .withColumn( "inserted_datetime", F.current_timestamp(), )
    )