import os
import re
import requests

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)


# ==================================================
# Configuration
# ==================================================

BLS_URL = "https://download.bls.gov/pub/time.series/pr/"

DATAUSA_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)

VOLUME_PATH = Path("/Volumes/workspace/raw/bls")

# Change these if your catalog/schema differ.
CATALOG = "workspace"
SCHEMA = "raw"
TABLE_NAME = "ingestion_metadata"
TABLE = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

HEADERS = {
    "User-Agent": "my-databricks-ingestion/1.0"
}

CHUNK_SIZE = 1024 * 1024  # 1 MB


# ==================================================
# Unity Catalog table
# ==================================================

VOLUME_PATH.mkdir(parents=True, exist_ok=True)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        source_name STRING,
        file_name STRING,
        source_url STRING,
        etag STRING,
        last_modified STRING,
        content_length STRING,
        downloaded_size_bytes BIGINT,
        last_successful_ingestion TIMESTAMP,
        last_checked_at TIMESTAMP,
        status STRING,
        error_message STRING
    )
    USING DELTA
""")


# ==================================================
# Helpers
# ==================================================

def utc_now():
    """Return a UTC timestamp suitable for Spark TimestampType."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_existing_metadata(source_name: str, file_name: str):
    df = spark.table(TABLE)

    rows = (
        df.filter(
            (df["source_name"] == source_name)
            & (df["file_name"] == file_name)
        )
        .limit(1)
        .collect()
    )

    return rows[0].asDict() if rows else None


def record_metadata(
    source_name,
    file_name,
    source_url,
    status,
    etag=None,
    last_modified=None,
    content_length=None,
    downloaded_size_bytes=None,
    last_successful_ingestion=None,
    error_message=None,
):
    """
    Insert or update the latest metadata record.

    Successful downloads update the source metadata and successful
    ingestion timestamp. Failed downloads preserve the last known
    successful metadata and timestamp.
    """

    row = {
        "source_name": source_name,
        "file_name": file_name,
        "source_url": source_url,
        "etag": etag,
        "last_modified": last_modified,
        "content_length": content_length,
        "downloaded_size_bytes": downloaded_size_bytes,
        "last_successful_ingestion": last_successful_ingestion,
        "last_checked_at": utc_now(),
        "status": status,
        "error_message": error_message,
    }

    schema = StructType([
        StructField("source_name", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("source_url", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("last_modified", StringType(), True),
        StructField("content_length", StringType(), True),
        StructField("downloaded_size_bytes", LongType(), True),
        StructField("last_successful_ingestion", TimestampType(), True),
        StructField("last_checked_at", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ])

    source_df = spark.createDataFrame([row], schema=schema)
    view_name = "incoming_ingestion_metadata"

    source_df.createOrReplaceTempView(view_name)

    # Preserve last known successful metadata on failures and
    # unchanged checks. Update metadata only after a new download.
    spark.sql(f"""
        MERGE INTO {TABLE} AS target
        USING {view_name} AS source
        ON target.source_name = source.source_name
           AND target.file_name = source.file_name

        WHEN MATCHED THEN UPDATE SET
            target.source_url = source.source_url,

            target.etag = CASE
                WHEN source.status = 'SUCCESS'
                THEN source.etag
                ELSE target.etag
            END,

            target.last_modified = CASE
                WHEN source.status = 'SUCCESS'
                THEN source.last_modified
                ELSE target.last_modified
            END,

            target.content_length = CASE
                WHEN source.status = 'SUCCESS'
                THEN source.content_length
                ELSE target.content_length
            END,

            target.downloaded_size_bytes = CASE
                WHEN source.status = 'SUCCESS'
                THEN source.downloaded_size_bytes
                ELSE target.downloaded_size_bytes
            END,

            target.last_successful_ingestion = CASE
                WHEN source.status = 'SUCCESS'
                THEN source.last_successful_ingestion
                ELSE target.last_successful_ingestion
            END,

            target.last_checked_at = source.last_checked_at,
            target.status = source.status,
            target.error_message = source.error_message

        WHEN NOT MATCHED THEN INSERT (
            source_name,
            file_name,
            source_url,
            etag,
            last_modified,
            content_length,
            downloaded_size_bytes,
            last_successful_ingestion,
            last_checked_at,
            status,
            error_message
        )
        VALUES (
            source.source_name,
            source.file_name,
            source.source_url,
            source.etag,
            source.last_modified,
            source.content_length,
            source.downloaded_size_bytes,
            source.last_successful_ingestion,
            source.last_checked_at,
            source.status,
            source.error_message
        )
    """)

    spark.catalog.dropTempView(view_name)


def metadata_matches(remote, previous):
    """
    Compare remote metadata with the last successful ingestion.

    Prefer ETag. Fall back to Last-Modified plus Content-Length
    when an ETag is unavailable.
    """

    if previous is None:
        return False

    if remote.get("etag") is not None:
        return (
            previous.get("etag") is not None
            and previous["etag"] == remote["etag"]
        )

    if (
        remote.get("last_modified") is not None
        and remote.get("content_length") is not None
    ):
        return (
            previous.get("last_modified")
            == remote["last_modified"]
            and previous.get("content_length")
            == remote["content_length"]
        )

    # Cannot reliably establish that the file is unchanged.
    return False


def download_response_to_file(response, output_path):
    """Stream a response to a temporary file and replace on success."""

    temp_path = output_path.with_name(output_path.name + ".tmp")

    try:
        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(
                chunk_size=CHUNK_SIZE
            ):
                if chunk:
                    f.write(chunk)

        os.replace(temp_path, output_path)

    finally:
        if temp_path.exists():
            temp_path.unlink()


# ==================================================
# BLS ingestion
# ==================================================

def ingest_bls_file(session, file_url, filename):
    output_path = VOLUME_PATH / filename
    source_name = "bls_pr"

    previous = get_existing_metadata(source_name, filename)

    # Conditional requests only make sense when we have both
    # a local file and previously recorded metadata.
    request_headers = {}

    if output_path.is_file() and previous is not None:
        if previous.get("etag"):
            request_headers["If-None-Match"] = previous["etag"]
        elif previous.get("last_modified"):
            request_headers["If-Modified-Since"] = (
                previous["last_modified"]
            )

    print(f"\nChecking: {filename}")

    try:
        with session.get(
            file_url,
            headers=request_headers,
            stream=True,
            timeout=120,
        ) as response:

            # Conditional GET: server confirms existing copy is current.
            if response.status_code == 304 and output_path.is_file():
                record_metadata(
                    source_name=source_name,
                    file_name=filename,
                    source_url=file_url,
                    status="UNCHANGED",
                )
                print("  Unchanged (HTTP 304); skipped.")
                return "skipped"

            response.raise_for_status()

            remote = {
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_length": response.headers.get("Content-Length"),
            }

            print(f"  Last-Modified: {remote['last_modified']}")
            print(f"  ETag: {remote['etag']}")
            print(f"  Content-Length: {remote['content_length']}")

            # Some servers ignore conditional request headers.
            # Compare response metadata before consuming the body.
            if (
                output_path.is_file()
                and metadata_matches(remote, previous)
            ):
                record_metadata(
                    source_name=source_name,
                    file_name=filename,
                    source_url=file_url,
                    status="UNCHANGED",
                )
                print("  Metadata matches; skipped download.")
                return "skipped"

            # New or changed file: download the response.
            download_response_to_file(response, output_path)

        size_bytes = output_path.stat().st_size
        successful_at = utc_now()

        record_metadata(
            source_name=source_name,
            file_name=filename,
            source_url=file_url,
            etag=remote["etag"],
            last_modified=remote["last_modified"],
            content_length=remote["content_length"],
            downloaded_size_bytes=size_bytes,
            last_successful_ingestion=successful_at,
            status="SUCCESS",
        )

        print(f"  Downloaded: {output_path} ({size_bytes:,} bytes)")
        print("  Metadata table updated.")

        return "downloaded"

    except (requests.RequestException, OSError) as e:
        record_metadata(
            source_name=source_name,
            file_name=filename,
            source_url=file_url,
            status="FAILED",
            error_message=str(e),
        )

        print(f"  ERROR: {e}")
        return "failed"


print("Fetching BLS directory listing...")

downloaded = 0
skipped = 0
failed = 0

with requests.Session() as session:
    session.headers.update(HEADERS)

    response = session.get(BLS_URL, timeout=60)
    response.raise_for_status()

    links = re.findall(
        r'href=["\']([^"\']+)["\']',
        response.text,
        re.IGNORECASE,
    )

    base_parsed = urlparse(BLS_URL)
    base_path = base_parsed.path.rstrip("/")
    base_host = base_parsed.netloc

    for link in links:
        file_url = urljoin(BLS_URL, link)
        parsed_url = urlparse(file_url)

        if link.endswith("/"):
            continue

        if parsed_url.scheme not in ("http", "https"):
            continue

        if parsed_url.netloc != base_host:
            continue

        if parsed_url.path.rsplit("/", 1)[0] != base_path:
            continue

        filename = Path(parsed_url.path).name

        if not filename or filename in {".", ".."}:
            continue

        result = ingest_bls_file(
            session,
            file_url,
            filename,
        )

        if result == "downloaded":
            downloaded += 1
        elif result == "skipped":
            skipped += 1
        else:
            failed += 1


print("\nBLS ingestion complete.")
print(f"Downloaded: {downloaded}")
print(f"Skipped: {skipped}")
print(f"Failed: {failed}")


# ==================================================
# DataUSA population ingestion
# ==================================================

print("\nDownloading DataUSA population data...")

population_path = VOLUME_PATH / "population.json"
population_source = "datausa_population"

try:
    with requests.Session() as session:
        session.headers.update(HEADERS)

        with session.get(
            DATAUSA_URL,
            stream=True,
            timeout=60,
        ) as response:
            response.raise_for_status()

            remote = {
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_length": response.headers.get("Content-Length"),
            }

            temp_path = population_path.with_name(
                population_path.name + ".tmp"
            )

            try:
                # Validate the JSON before replacing the existing file.
                # Download the response into a temporary file first.
                with open(temp_path, "wb") as f:
                    for chunk in response.iter_content(
                        chunk_size=CHUNK_SIZE
                    ):
                        if chunk:
                            f.write(chunk)

                import json

                with open(temp_path, "r", encoding="utf-8") as f:
                    json.load(f)

                os.replace(temp_path, population_path)

            finally:
                if temp_path.exists():
                    temp_path.unlink()

    record_metadata(
        source_name=population_source,
        file_name="population.json",
        source_url=DATAUSA_URL,
        etag=remote["etag"],
        last_modified=remote["last_modified"],
        content_length=remote["content_length"],
        downloaded_size_bytes=population_path.stat().st_size,
        last_successful_ingestion=utc_now(),
        status="SUCCESS",
    )

    print(f"Saved: {population_path}")
    print("DataUSA metadata table updated.")

except (requests.RequestException, OSError, ValueError) as e:
    record_metadata(
        source_name=population_source,
        file_name="population.json",
        source_url=DATAUSA_URL,
        status="FAILED",
        error_message=str(e),
    )

    print(f"ERROR downloading population data: {e}")


# ==================================================
# Summary
# ==================================================

print(f"\nMetadata table: {TABLE}")
print("Ingestion run finished.")

display(
    spark.table(TABLE)
    .orderBy("source_name", "file_name")
)
