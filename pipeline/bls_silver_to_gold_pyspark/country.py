from pyspark import pipelines as dp
from pyspark.sql import functions as F


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
        .withColumn( "inserted_datetime", F.current_timestamp(), )
    )