import hashlib
import json
import os
import re
import requests

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)


# ==================================================
# Configuration
# ==================================================

BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
DATAUSA_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation&locale=en&measures=Population"
)

VOLUME_PATH = Path("/Volumes/workspace/raw/bls")
TABLE = "workspace.raw.ingestion_metadata"
HEADERS = {"User-Agent": "my-databricks-ingestion/1.0"}
CHUNK_SIZE = 1024 * 1024

SCHEMA = StructType([
    StructField("source_name", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("source_url", StringType()),
    StructField("etag", StringType()),
    StructField("last_modified", StringType()),
    StructField("content_length", StringType()),
    StructField("downloaded_size_bytes", LongType()),
    StructField("last_successful_ingestion", TimestampType()),
    StructField("last_checked_at", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("error_message", StringType()),
])

VOLUME_PATH.mkdir(parents=True, exist_ok=True)
spark.catalog.createDatabase("workspace.raw", ifNotExists=True)

(
    spark.createDataFrame([], SCHEMA)
    .write.format("delta")
    .mode("ignore")
    .saveAsTable(TABLE)
)


# ==================================================
# Metadata helpers
# ==================================================

def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_metadata(source, filename):
    rows = (
        spark.table(TABLE)
        .filter(
            (F.col("source_name") == source)
            & (F.col("file_name") == filename)
        )
        .limit(1)
        .collect()
    )
    return rows[0].asDict() if rows else None


def record_metadata(source, filename, url, status, remote=None,
                    size=None, error=None):
    remote = remote or {}
    previous = get_metadata(source, filename)

    successful = status == "SUCCESS"

    row = {
        "source_name": source,
        "file_name": filename,
        "source_url": url,
        "etag": remote.get("etag"),
        "last_modified": remote.get("last_modified"),
        "content_length": remote.get("content_length"),
        "downloaded_size_bytes": size,
        "last_successful_ingestion": now() if successful else None,
        "last_checked_at": now(),
        "status": status,
        "error_message": error,
    }

    incoming = spark.createDataFrame([row], SCHEMA).alias("s")
    target = DeltaTable.forName(spark, TABLE).alias("t")

    condition = (
        (F.col("t.source_name") == F.col("s.source_name"))
        & (F.col("t.file_name") == F.col("s.file_name"))
    )

    updates = {
        "source_url": F.col("s.source_url"),
        "last_checked_at": F.col("s.last_checked_at"),
        "status": F.col("s.status"),
        "error_message": F.col("s.error_message"),
    }

    for field in (
        "etag", "last_modified", "content_length",
        "downloaded_size_bytes", "last_successful_ingestion"
    ):
        updates[field] = F.when(
            F.col("s.status") == "SUCCESS", F.col(f"s.{field}")
        ).otherwise(F.col(f"t.{field}"))

    (
        target.merge(incoming, condition)
        .whenMatchedUpdate(set=updates)
        .whenNotMatchedInsert(
            values={c: F.col(f"s.{c}") for c in SCHEMA.fieldNames()}
        )
        .execute()
    )


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ==================================================
# Download helper
# ==================================================

def download(session, url, path, source, validate_json=False):
    """Download only changed content; preserve existing files on failure."""

    filename = path.name
    previous = get_metadata(source, filename)
    headers = {}

    if path.is_file() and previous:
        if previous.get("etag"):
            headers["If-None-Match"] = previous["etag"]
        elif previous.get("last_modified"):
            headers["If-Modified-Since"] = previous["last_modified"]

    temp = path.with_name(path.name + ".tmp")
    print(f"\nChecking: {filename}")

    try:
        with session.get(
            url, headers=headers, stream=True, timeout=120
        ) as response:

            if response.status_code == 304 and path.is_file():
                record_metadata(source, filename, url, "UNCHANGED")
                print("  Unchanged; skipped.")
                return "skipped"

            response.raise_for_status()

            remote = {
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_length": response.headers.get("Content-Length"),
            }

            with open(temp, "wb") as file:
                for chunk in response.iter_content(CHUNK_SIZE):
                    if chunk:
                        file.write(chunk)

        if validate_json:
            with open(temp, encoding="utf-8") as file:
                json.load(file)

        unchanged = (
            path.is_file()
            and sha256(path) == sha256(temp)
        ) if validate_json else (
            path.is_file()
            and previous is not None
            and (
                (
                    remote["etag"] is not None
                    and previous.get("etag") == remote["etag"]
                )
                or (
                    remote["etag"] is None
                    and remote["last_modified"] is not None
                    and remote["content_length"] is not None
                    and previous.get("last_modified") == remote["last_modified"]
                    and previous.get("content_length") == remote["content_length"]
                )
            )
        )

        if unchanged and previous:
            record_metadata(source, filename, url, "UNCHANGED")
            print("  Unchanged; existing file preserved.")
            return "skipped"

        os.replace(temp, path)

        record_metadata(
            source, filename, url, "SUCCESS",
            remote=remote, size=path.stat().st_size
        )
        print(f"  Downloaded: {path} ({path.stat().st_size:,} bytes)")
        return "downloaded"

    except (requests.RequestException, OSError, ValueError) as error:
        record_metadata(
            source, filename, url, "FAILED", error=str(error)
        )
        print(f"  ERROR: {error}")
        return "failed"

    finally:
        if temp.exists():
            temp.unlink()


# ==================================================
# BLS ingestion
# ==================================================

print("Fetching BLS directory listing...")

downloaded = skipped = failed = 0

with requests.Session() as session:
    session.headers.update(HEADERS)
    response = session.get(BLS_URL, timeout=60)
    response.raise_for_status()

    links = re.findall(r'href=["\']([^"\']+)["\']',
                       response.text, re.IGNORECASE)

    base = urlparse(BLS_URL)

    for link in links:
        url = urljoin(BLS_URL, link)
        parsed = urlparse(url)

        if (
            link.endswith("/")
            or parsed.scheme not in ("http", "https")
            or parsed.netloc != base.netloc
            or parsed.path.rsplit("/", 1)[0] != base.path.rstrip("/")
        ):
            continue

        filename = Path(parsed.path).name
        if not filename or filename in {".", ".."}:
            continue

        result = download(
            session, url, VOLUME_PATH / filename, "bls_pr"
        )

        if result == "downloaded":
            downloaded += 1
        elif result == "skipped":
            skipped += 1
        else:
            failed += 1

print(f"\nBLS complete: {downloaded} downloaded, "
      f"{skipped} skipped, {failed} failed.")


# ==================================================
# DataUSA population ingestion
# ==================================================

print("\nChecking DataUSA population data...")

with requests.Session() as session:
    session.headers.update(HEADERS)
    result = download(
        session,
        DATAUSA_URL,
        VOLUME_PATH / "population.json",
        "datausa_population",
        validate_json=True,
    )

print(f"DataUSA result: {result}")
