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
async def download_file(temp_location: str, filename: str) -> str:
    target_host = (
        "https://github.com/sapics/ip-location-db/raw/refs/heads/main/dbip-city/"
    )

    if "DOWNLOAD_HOST" in os.environ:
        target_host = os.environ.get("DOWNLOAD_HOST")

    with requests.get(target_host + filename, stream=True) as r:
        r.raise_for_status()
        with open(temp_location + filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return filename


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
async def clickhouse_create_geoip_shared_table() -> str:
    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        database=os.environ["CLICKHOUSE_DATABASE"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        username=os.environ["CLICKHOUSE_USERNAME"],
    )

    client.command("""
        CREATE OR REPLACE TABLE geoip (
           cidr String,
           latitude Float64,
           longitude Float64,
           country_code String
        )
        ENGINE = MergeTree()
        ORDER BY cidr;
    """)

    return "geoip"


@activity.defn
async def clickhouse_insert_geoip_shared_table_records(ip_family: str) -> str:
    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        database=os.environ["CLICKHOUSE_DATABASE"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        username=os.environ["CLICKHOUSE_USERNAME"],
    )

    SQL_TEMPLATE = """
        INSERT INTO geoip
        WITH
            bitXor(ip_range_start, ip_range_end) AS xor,
            if(xor != 0, {unmatched_expr}, 0) AS unmatched,
            {bit_width} - unmatched AS cidr_suffix,
            {cidr_address_expr} AS cidr_address
        SELECT
            concat(toString(cidr_address), '/', toString(cidr_suffix)) AS cidr,
            latitude,
            longitude,
            country_code
        FROM geoip.{source_table};
    """

    GEOIP_SQL_PARAMS = {
        "IPv4": {
            "bit_width": 32,
            "unmatched_expr": "ceil(log2(xor))",
            "cidr_address_expr": """
                toIPv4(
                    bitAnd(
                        bitNot(pow(2, unmatched) - 1),
                        ip_range_start
                    )::UInt64
                )
            """,
            "source_table": "geoip_ipv4",
        },
        "IPv6": {
            "bit_width": 128,
            "unmatched_expr": "toUInt8(ceil(log2(xor)))",
            "cidr_address_expr": """
                CAST(
                    reverse(
                        reinterpretAsFixedString(
                            bitAnd(
                                bitNot(
                                    bitShiftRight(
                                        toUInt128(bitNot(0)),
                                        cidr_suffix
                                    )
                                ),
                                ip_range_start
                            )
                        )
                    ) AS IPv6
                )
            """,
            "source_table": "geoip_ipv6",
        },
    }

    params = GEOIP_SQL_PARAMS[ip_family]
    query = SQL_TEMPLATE.format(**params)
    client.command(query)

    return f"geoip_{ip_family.lower()}"


@activity.defn
async def clickhouse_create_geoip_records_table(ip_family: str) -> str:
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
async def clickhouse_insert_geoip_records(ip_family: str, filename: str) -> int:
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
