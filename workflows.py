from datetime import timedelta
from temporalio import workflow


# Import activity, passing it through the sandbox without reloading the module
with workflow.unsafe.imports_passed_through():
    from activities import (
        create_temp_location,
        delete_temp_location,
        download_file,
        decompress_file,
        clickhouse_create_geoip_cidr_table,
        clickhouse_create_geoip_raw_records_table,
        clickhouse_insert_geoip_raw_records,
        clickhouse_insert_geoip_shared_table_records,
    )


@workflow.defn
class ClickHouseGeoIPDataInsert:
    @workflow.run
    async def run(self, temp_location: str, ip_family: str) -> int:
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
        await workflow.execute_activity(
            clickhouse_insert_geoip_shared_table_records,
            ip_family,
            start_to_close_timeout=timedelta(seconds=60),
        )

        return records


@workflow.defn
class ClickHouseGeoIPImport:
    @workflow.run
    async def run(self):
        temp_location = await workflow.execute_activity(
            create_temp_location, start_to_close_timeout=timedelta(seconds=60)
        )

        await workflow.execute_activity(
            clickhouse_create_geoip_cidr_table,
            start_to_close_timeout=timedelta(seconds=60),
        )

        for ip_family in ["IPv4", "IPv6"]:
            await workflow.execute_child_workflow(
                ClickHouseGeoIPDataInsert.run,
                args=[temp_location, ip_family],
                id=f"clickhouse-geoip-data-insert-{ip_family.lower()}",
            )
