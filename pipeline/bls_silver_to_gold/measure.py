from pyspark import pipelines as dp


@dp.table(name="measure")
def create_gold_measure_table():

    return (
        spark.read.table("workspace.silver.silver_pr_measure")
        .select(
            "measure_code",
            "measure_name",
        )
        .dropDuplicates([
            "measure_code",
        ])
    )