from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.table(name="data")
def create_gold_data_table():

    silver_all_data = spark.read.table(
        "workspace.silver.silver_pr_data_1_alldata"
    )
    silver_all_data = silver_all_data.withColumn("_priority", F.lit(2))
    silver_current_data = spark.read.table(
        "workspace.silver.silver_pr_data_0_current"
    )
    silver_current_data = silver_current_data.withColumn("_priority", F.lit(1))
    silver_data = silver_current_data.unionByName(silver_all_data)
    window = (
        Window
        .partitionBy("series_id", "year", "quarter")
        .orderBy("_priority")
    )
    result = (
        silver_data
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_priority", "_rank")
    )
    silver_series = spark.read.table(
        "workspace.silver.silver_pr_series"
    )

    return (
        silver_data
        .join(
            silver_series,
            silver_data.series_id == silver_series.series_id,
            "left",
        )
        .select(
            silver_data.series_id,
            silver_data.year,
            silver_data.quarter,
            silver_data.value,
            silver_series.sector_code,
            silver_series.class_code,
            silver_series.measure_code,
            silver_series.duration_code,
            silver_series.seasonal,
        )
        .withColumn( "inserted_datetime", F.current_timestamp(), )
    )