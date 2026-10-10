from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="population")
def create_gold_population_table():

    return (
        spark.read.table("workspace.silver.silver_population_json")
        .select(
            "nation_id",
            "year",
            "population",
        )
        .dropDuplicates([
            "nation_id",
            "year",
        ])
        .withColumn( "inserted_datetime", F.current_timestamp(), )
    )