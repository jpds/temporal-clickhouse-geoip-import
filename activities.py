from temporalio import activity

import clickhouse_connect
import csv
import gzip
import io
import os
import requests
import shutil
import tempfile


@activity.defn
async def create_temp_location() -> str:
    tempdir = tempfile.TemporaryDirectory(
        prefix="clickhouse-geoip-import", delete=False
    )
    return tempdir.name + "/"


@activity.defn
async def delete_temp_location(temp_location: str):
    shutil.rmtree(temp_location)


@activity.defn
async def download_file(temp_location: str, url: str) -> str:
    local_filename = url.split("/")[-1]

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(temp_location + local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return local_filename


@activity.defn
async def decompress_file(temp_location: str, filename: str) -> str:
    out_path, ext = os.path.splitext(filename)

    with gzip.open(temp_location + filename, "rb") as gz:
        with io.BufferedReader(gz) as buffered_reader:
            with open(temp_location + out_path, "wb") as out_file:
                while True:
                    chunk = buffered_reader.read(1024 * 1024)  # 1 MB chunks
                    if not chunk:
                        break
                    out_file.write(chunk)

    return temp_location + out_path


@activity.defn
async def clickhouse_create_table_geoip(ip_family: str) -> str:
    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        database=os.environ["CLICKHOUSE_DATABASE"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        username=os.environ["CLICKHOUSE_USERNAME"],
    )

    client.command(f"""
        CREATE OR REPLACE TABLE geoip_{ip_family.lower()}
        (
            `ip_range_start` {ip_family},
            `ip_range_end` {ip_family},
            `country_code` Nullable(String),
            `state1` Nullable(String),
            `state2` Nullable(String),
            `city` Nullable(String),
            `postcode` Nullable(String),
            `latitude` Float64,
            `longitude` Float64,
            `timezone` Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY ip_range_start
    """)

    return f"geoip_{ip_family.lower()}"


@activity.defn
async def clickhouse_import_geoip(ip_family: str, filename: str) -> int:
    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        database=os.environ["CLICKHOUSE_DATABASE"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        username=os.environ["CLICKHOUSE_USERNAME"],
    )

    with open(filename, "r") as csvfile:
        reader = csv.reader(csvfile)

        rows_to_insert = []

        for row in reader:
            row_data = (
                row[0],  # ip_range_start (IPv4/IPv6)
                row[1],  # ip_range_end (IPv4/IPv6)
                row[2] or None,  # country_code (Nullable String)
                row[3] or None,  # state1 (Nullable String)
                row[4] or None,  # state2 (Nullable String)
                row[5] or None,  # city (Nullable String)
                row[6] or None,  # postcode (Nullable String)
                float(row[7]) if row[7] else None,  # latitude
                float(row[8]) if row[8] else None,  # longitude
                row[9] or None,  # timezone (Nullable String)
            )

            rows_to_insert.append(row_data)

    client.insert(f"geoip_{ip_family.lower()}", rows_to_insert)

    return len(rows_to_insert)
