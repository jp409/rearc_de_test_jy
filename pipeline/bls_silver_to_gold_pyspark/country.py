from pyspark import pipelines as dp


@dp.table(name="country")
def create_gold_country_table():

    return (
        spark.read.table("workspace.silver.silver_population_json")
        .select(
            "nation_id",
            "nation",
        )
        .dropDuplicates([
            "nation_id",
        ])
    )