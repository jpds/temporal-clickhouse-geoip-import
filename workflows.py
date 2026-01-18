from datetime import timedelta
from temporalio import workflow


import asyncio


# Import activity, passing it through the sandbox without reloading the module
with workflow.unsafe.imports_passed_through():
    from activities import (
        create_temp_location,
        download_file,
        decompress_file,
        clickhouse_create_geoip_cidr_table,
        clickhouse_create_geoip_raw_records_table,
        clickhouse_insert_geoip_raw_records,
        clickhouse_insert_geoip_shared_table_records,
    )


@workflow.defn
class ClickHouseGeoIPRawInsert:
    @workflow.run
    async def run(self, temp_location: str, ip_family: str) -> str:
        downloaded_file = await workflow.execute_activity(
            download_file,
            args=[temp_location, f"dbip-city-{ip_family.lower()}.csv.gz"],
            start_to_close_timeout=timedelta(seconds=60),
        )
        decompressed_file = await workflow.execute_activity(
            decompress_file,
            args=[temp_location, downloaded_file],
            start_to_close_timeout=timedelta(seconds=60),
        )
        await workflow.execute_activity(
            clickhouse_create_geoip_raw_records_table,
            ip_family,
            start_to_close_timeout=timedelta(seconds=60),
        )
        records = await workflow.execute_activity(
            clickhouse_insert_geoip_raw_records,
            args=[ip_family, decompressed_file],
            start_to_close_timeout=timedelta(seconds=60),
        )
        return records


@workflow.defn
class ClickHouseGeoIPSharedTableInsert:
    @workflow.run
    async def run(self, ip_family: str):
        await workflow.execute_activity(
            clickhouse_insert_geoip_shared_table_records,
            ip_family,
            start_to_close_timeout=timedelta(seconds=60),
        )


@workflow.defn
class ClickHouseGeoIPImport:
    @workflow.run
    async def run(self):
        temp_location = await workflow.execute_activity(
            create_temp_location, start_to_close_timeout=timedelta(seconds=60)
        )

        ipv4_raw_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPRawInsert.run,
            args=[temp_location, "IPv4"],
            id="clickhouse-geoip-raw-insert-ipv4",
        )
        ipv6_raw_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPRawInsert.run,
            args=[temp_location, "IPv6"],
            id="clickhouse-geoip-raw-insert-ipv6",
        )
        await asyncio.gather(ipv4_raw_insert, ipv6_raw_insert)

        await workflow.execute_activity(
            clickhouse_create_geoip_cidr_table,
            start_to_close_timeout=timedelta(seconds=60),
        )

        ipv4_shared_table_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPSharedTableInsert.run,
            "IPv4",
            id="clickhouse-geoip-shared-table-insert-ipv4",
        )
        ipv6_shared_table_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPSharedTableInsert.run,
            "IPv6",
            id="clickhouse-geoip-shared-table-insert-ipv6",
        )
        await asyncio.gather(ipv4_shared_table_insert, ipv6_shared_table_insert)

        return "GeoIP import completed"
