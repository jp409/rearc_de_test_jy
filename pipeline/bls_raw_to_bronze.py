from pyspark import pipelines as dp
import os


BASE_PATH = "/Volumes/workspace/raw/bls"

def create_bronze_table(file_name):
    table_name = "bronze_" + file_name.replace(".", "_")

    @dp.table(name=table_name)
    def bronze_table(source_file=file_name):
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("pathGlobFilter", source_file)
            .load(BASE_PATH)
        )

# Discover all files in the BLS volume
files = sorted(
    file_name
    for file_name in os.listdir(BASE_PATH)
    if os.path.isfile(os.path.join(BASE_PATH, file_name))
)

# Create one Bronze table per file
for file_name in files:
    create_bronze_table(file_name)