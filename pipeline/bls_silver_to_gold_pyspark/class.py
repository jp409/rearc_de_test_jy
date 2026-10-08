from pyspark import pipelines as dp


@dp.table(name="class")
def create_gold_class_table():

    return (
        spark.read.table("workspace.silver.silver_pr_class")
        .select(
            "class_code",
            "class_text",
        )
        .dropDuplicates([
            "class_code",
        ])
    )