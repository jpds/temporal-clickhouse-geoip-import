from datetime import timedelta
from temporalio import workflow


import asyncio


# Import activity, passing it through the sandbox without reloading the module
with workflow.unsafe.imports_passed_through():
    from activities import (
        create_temp_location,
        delete_temp_location,
        download_file,
        decompress_file,
        clickhouse_create_geoip_records_table,
        clickhouse_create_geoip_shared_table,
        clickhouse_drop_geoip_records_table,
        clickhouse_drop_geoip_shared_table,
        clickhouse_exchange_geoip_table,
        clickhouse_insert_geoip_records,
        clickhouse_insert_geoip_shared_table_records,
        read_geoip_dataset_version,
    )


@workflow.defn
class ClickHouseGeoIPInsert:
    @workflow.run
    async def run(self, temp_location: str, ip_family: str, version: str) -> int:
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
        import_table = await workflow.execute_activity(
            clickhouse_create_geoip_records_table,
            args=[ip_family, version],
            start_to_close_timeout=timedelta(seconds=60),
        )
        records = await workflow.execute_activity(
            clickhouse_insert_geoip_records,
            args=[import_table, decompressed_file],
            start_to_close_timeout=timedelta(seconds=600),
        )
        return records


@workflow.defn
class ClickHouseGeoIPSharedTableInsert:
    @workflow.run
    async def run(self, ip_family: str, version: str):
        await workflow.execute_activity(
            clickhouse_insert_geoip_shared_table_records,
            args=[ip_family, version],
            start_to_close_timeout=timedelta(seconds=600),
        )


@workflow.defn
class ClickHouseGeoIPImport:
    @workflow.run
    async def run(self):
        version = await workflow.execute_activity(
            read_geoip_dataset_version, start_to_close_timeout=timedelta(seconds=60)
        )
        temp_location = await workflow.execute_activity(
            create_temp_location, version, start_to_close_timeout=timedelta(seconds=60)
        )

        ipv4_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPInsert.run,
            args=[temp_location, "IPv4", version],
            id="clickhouse-geoip-insert-ipv4",
        )
        ipv6_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPInsert.run,
            args=[temp_location, "IPv6", version],
            id="clickhouse-geoip-insert-ipv6",
        )
        ipv4_records, ipv6_records = await asyncio.gather(ipv4_insert, ipv6_insert)

        new_geoip_table = await workflow.execute_activity(
            clickhouse_create_geoip_shared_table,
            version,
            start_to_close_timeout=timedelta(seconds=600),
        )

        ipv4_shared_table_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPSharedTableInsert.run,
            args=["IPv4", version],
            id="clickhouse-geoip-shared-table-insert-ipv4",
        )
        ipv6_shared_table_insert = workflow.execute_child_workflow(
            ClickHouseGeoIPSharedTableInsert.run,
            args=["IPv6", version],
            id="clickhouse-geoip-shared-table-insert-ipv6",
        )
        await asyncio.gather(ipv4_shared_table_insert, ipv6_shared_table_insert)

        await workflow.execute_activity(
            delete_temp_location,
            temp_location,
            start_to_close_timeout=timedelta(seconds=600),
        )

        await workflow.execute_activity(
            clickhouse_exchange_geoip_table,
            new_geoip_table,
            start_to_close_timeout=timedelta(seconds=600),
        )

        await asyncio.gather(
            workflow.execute_activity(
                clickhouse_drop_geoip_shared_table,
                new_geoip_table,
                start_to_close_timeout=timedelta(seconds=600),
            ),
            workflow.execute_activity(
                clickhouse_drop_geoip_records_table,
                args=["IPv4", version],
                start_to_close_timeout=timedelta(seconds=600),
            ),
            workflow.execute_activity(
                clickhouse_drop_geoip_records_table,
                args=["IPv6", version],
                start_to_close_timeout=timedelta(seconds=600),
            ),
        )

        return f"GeoIP import for version {version} completed: inserted {ipv4_records} IPv4 and {ipv6_records} IPv6 records"
