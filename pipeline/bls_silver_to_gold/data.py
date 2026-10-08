from pyspark import pipelines as dp


@dp.table(name="data")
def create_gold_data_table():

    silver_data = spark.read.table(
        "workspace.silver.silver_pr_data_0_current"
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
            silver_data.year,
            silver_data.quarter,
            silver_data.value,
            silver_series.sector_code,
            silver_series.class_code,
            silver_series.measure_code,
            silver_series.duration_code,
            silver_series.seasonal,
        )
    )