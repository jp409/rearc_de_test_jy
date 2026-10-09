import os
import yaml

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    StructType,
    StructField,
    ArrayType,
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Adjust this to wherever your YAML configs live
CONFIG_DIR = "/Workspace/Repos/jayyurcho@gmail.com/rearc_de_test_jy/config"


# --------------------------------------------------
# Type mapping
# --------------------------------------------------

TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
}


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def load_configs():
    """
    Dynamically discover all YAML configs.

    A config file named:
        pr_class.yaml

    corresponds to:
        pr.class
    """

    configs = []

    for filename in sorted(os.listdir(CONFIG_DIR)):
        if not filename.endswith((".yaml", ".yml")):
            continue

        config_path = os.path.join(CONFIG_DIR, filename)

        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}

        source = config.get("source", {})
        source_file = source.get("file")

        if not source_file:
            raise ValueError(
                f"Config '{filename}' is missing source.file"
            )

        configs.append(
            {
                "config_file": filename,
                "source_file": source_file,
                "config": config,
            }
        )

    return configs


def table_suffix(source_file):
    """
    Convert source filename to the Bronze/Silver table suffix.

    pr.class -> pr_class
    pr.measure -> pr_measure
    population.json -> population_json
    """

    return source_file.replace(".", "_").replace("-", "_")


def spark_type(type_name):
    """
    Convert YAML type name into a Spark DataType.
    """

    normalized = type_name.lower()

    if normalized not in TYPE_MAP:
        raise ValueError(
            f"Unsupported type '{type_name}'. "
            f"Supported types: {sorted(TYPE_MAP.keys())}"
        )

    return TYPE_MAP[normalized]


def normalize_value(column, type_name):
    """
    For string columns:
      - Trim leading and trailing whitespace.
      - Convert empty strings to NULL.

    Other types are returned unchanged before casting.
    """
    if type_name.lower() == "string":
        trimmed = F.trim(column)

        return F.when(
            trimmed == "",
            F.lit(None).cast(StringType()),
        ).otherwise(trimmed)

    return column


def cast_column(column, column_config):
    """
    Normalize string values, then apply the configured Spark type.
    """
    type_name = column_config["type"]

    normalized = normalize_value(column, type_name)

    return normalized.cast(spark_type(type_name))


def apply_nullability_filter(df, columns_config):
    """
    Remove rows where a required Silver column is NULL.

    Nullable columns are left untouched.
    """

    for column_name, column_config in columns_config.items():
        nullable = column_config.get("nullable", True)

        if not nullable:
            df = df.filter(F.col(column_name).isNotNull())

    return df


# --------------------------------------------------
# Text / tab-delimited transformation
# --------------------------------------------------

def transform_text(bronze_df, silver_config):
    """
    Transform a Bronze table containing:

        value

    where value is a tab-delimited source row.
    """

    columns_config = silver_config["columns"]
    has_header = silver_config.get("has_header", False)

    column_names = list(columns_config.keys())

    # Split the raw line into fields.
    parts = F.split(F.col("value"), "\t")

    df = bronze_df

    # --------------------------------------------------
    # Remove header row
    # --------------------------------------------------

    if has_header:
        first_column = column_names[0]

        # Compare the first token to the configured first
        # Silver column name.
        first_value = F.lower(
            F.trim(
                F.regexp_replace(
                    parts.getItem(0),
                    '^"|"$',
                    "",
                )
            )
        )

        df = df.filter(
            first_value != F.lit(first_column.lower())
        )

    # --------------------------------------------------
    # Build typed Silver columns
    # --------------------------------------------------

    select_exprs = []

    for index, (column_name, column_config) in enumerate(
        columns_config.items()
    ):
        raw_column = parts.getItem(index)

        select_exprs.append(
            cast_column(
                raw_column,
                column_config,
            ).alias(column_name)
        )

    df = df.select(*select_exprs)

    # --------------------------------------------------
    # Required-column null filtering
    # --------------------------------------------------

    df = apply_nullability_filter(
        df,
        columns_config,
    )

    return df


# --------------------------------------------------
# JSON transformation
# --------------------------------------------------

def transform_json(bronze_df, silver_config):
    """
    Transform a Bronze table containing a JSON document.

    Expected structure:

    {
        ...
        "data": [
            {
                "Nation ID": "...",
                "Nation": "...",
                "Year": 2024,
                "Population": 123
            }
        ]
    }
    """

    columns_config = silver_config["columns"]

    # --------------------------------------------------
    # Build schema for the objects inside "data"
    # --------------------------------------------------

    json_fields = []

    for silver_column, column_config in columns_config.items():
        source_column = column_config.get(
            "source",
            silver_column,
        )

        json_fields.append(
            StructField(
                source_column,
                spark_type(column_config["type"]),
                True,
            )
        )

    data_schema = ArrayType(
        StructType(json_fields),
        True,
    )

    root_schema = StructType(
        [
            StructField(
                "data",
                data_schema,
                True,
            )
        ]
    )

    # --------------------------------------------------
    # Parse JSON
    # --------------------------------------------------

    parsed = bronze_df.select(
        F.from_json(
            F.col("value"),
            root_schema,
        ).alias("json")
    )

    # --------------------------------------------------
    # Explode data[] into rows
    # --------------------------------------------------

    exploded = parsed.select(
        F.explode(
            F.col("json.data")
        ).alias("record")
    )

    # --------------------------------------------------
    # Select / rename columns
    # --------------------------------------------------

    select_exprs = []

    for silver_column, column_config in columns_config.items():
        source_column = column_config.get(
            "source",
            silver_column,
        )

        select_exprs.append(
            F.col(
                f"record.`{source_column}`"
            ).alias(silver_column)
        )

    df = exploded.select(*select_exprs)

    # --------------------------------------------------
    # Required-column null filtering
    # --------------------------------------------------

    df = apply_nullability_filter(
        df,
        columns_config,
    )

    return df


# --------------------------------------------------
# Dataset factory
# --------------------------------------------------

def create_silver_table(source_file, config):
    silver_config = config.get("silver", {})

    if not silver_config.get("enabled", False):
        return

    source_config = config.get("source", {})
    source_format = source_config.get("format", "text").lower()

    suffix = source_file.replace(".", "_")

    bronze_table = f"workspace.bronze.bronze_{suffix}"
    silver_table = f"silver_{suffix}"

    @dp.table(name=silver_table)
    def silver_table_definition(
        bronze_table=bronze_table,
        silver_config=silver_config,
        source_format=source_format,
    ):
        bronze_df = spark.read.table(bronze_table)

        if source_format == "json":
            return transform_json(
                bronze_df,
                silver_config,
            )

        if source_format in ("text", "tsv", "tab"):
            return transform_text(
                bronze_df,
                silver_config,
            )

        raise ValueError(
            f"Unsupported source format: {source_format}"
        )


# --------------------------------------------------
# Create all configured Silver tables
# --------------------------------------------------

for dataset in load_configs():
    create_silver_table(
        dataset["source_file"],
        dataset["config"],
    )