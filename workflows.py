from datetime import timedelta
from temporalio import workflow


import asyncio


# Import activity, passing it through the sandbox without reloading the module
with workflow.unsafe.imports_passed_through():
    from activities import (
        create_temp_location,
        download_file,
        decompress_file,
        clickhouse_create_geoip_records_table,
        clickhouse_create_geoip_shared_table,
        clickhouse_insert_geoip_records,
        clickhouse_insert_geoip_shared_table_records,
    )


@workflow.defn
class ClickHouseGeoIPInsert:
    @workflow.run
    async def run(self, temp_location: str, ip_family: str) -> int:
        downloaded_file = await workflow.execute_activity(
            download_file,
            args=[temp_location, f"dbip-city-{ip_family.lower()}.csv.gz"],
            start_to_close_timeout=timedelta(seconds=600),
        )
        decompressed_file = await workflow.execute_activity(
            decompress_file,
            args=[temp_location, downloaded_file],
            start_to_close_timeout=timedelta(seconds=600),
        )
        await workflow.execute_activity(
            clickhouse_create_geoip_records_table,
            ip_family,
            start_to_close_timeout=timedelta(seconds=60),
        )
        records = await workflow.execute_activity(
            clickhouse_insert_geoip_records,
            args=[ip_family, decompressed_file],
            start_to_close_timeout=timedelta(seconds=600),
        )
        return records


@workflow.defn
class ClickHouseGeoIPSharedTableInsert:
    @workflow.run
    async def run(self, ip_family: str):
        await workflow.execute_activity(
            clickhouse_insert_geoip_shared_table_records,
            ip_family,
            start_to_close_timeout=timedelta(seconds=600),
        )


@workflow.defn
class ClickHouseGeoIPImport:
    @workflow.run
    async def run(self):
        temp_location = await workflow.execute_activity(
            create_temp_location, start_to_close_timeout=timedelta(seconds=60)
        )

        ipv4_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPInsert.run,
            args=[temp_location, "IPv4"],
            id="clickhouse-geoip-insert-ipv4",
        )
        ipv6_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPInsert.run,
            args=[temp_location, "IPv6"],
            id="clickhouse-geoip-insert-ipv6",
        )
        ipv4_records, ipv6_records = await asyncio.gather(ipv4_insert, ipv6_insert)

        await workflow.execute_activity(
            clickhouse_create_geoip_shared_table,
            start_to_close_timeout=timedelta(seconds=600),
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

        return f"GeoIP import completed: inserted {ipv4_records} IPv4 and {ipv6_records} IPv6 records"
