import os
import yaml

from pyspark import pipelines as dp

CONFIG_DIR = "/Workspace/Repos/jayyurcho@gmail.com/rearc_de_test_jy/config/gold"


def load_configs():
    configs = []

    for filename in os.listdir(CONFIG_DIR):
        if filename.endswith((".yaml", ".yml")):
            with open(os.path.join(CONFIG_DIR, filename), "r") as f:
                configs.append(yaml.safe_load(f))

    return configs


def register_table(config):
    table_name = config["table_name"]
    sql = config["sql"]

    def query():
        return spark.sql(sql)

    query.__name__ = f"gold_{table_name}"

    dp.table(name=table_name)(query)


for config in load_configs():
    register_table(config)