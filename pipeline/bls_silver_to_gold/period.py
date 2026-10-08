from pyspark import pipelines as dp


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
    )