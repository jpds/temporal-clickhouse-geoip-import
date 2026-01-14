from datetime import timedelta
from temporalio import workflow

# Import activity, passing it through the sandbox without reloading the module
with workflow.unsafe.imports_passed_through():
    from activities import (
        create_temp_location,
        delete_temp_location,
        download_file,
        decompress_file,
        clickhouse_create_table_geoip,
        clickhouse_import_geoip,
    )


@workflow.defn
class ClickHouseGeoIPImport:
    @workflow.run
    async def run(self, input: dict) -> int:
        # Validate IP family has been provided
        if "ip_family" not in input:
            raise ValueError("No IP family provided in input")

        if input["ip_family"] not in ["IPv4", "IPv6"]:
            raise ValueError(
                f"Invalid IP family provided in input: {input['ip_family']}"
            )

        target_url = f"https://github.com/sapics/ip-location-db/raw/refs/heads/main/dbip-city/dbip-city-{input['ip_family'].lower()}.csv.gz"

        # Check if user has overridden URL in input
        if "url" in input:
            target_url = input["url"]

        temp_location = await workflow.execute_activity(
            create_temp_location, start_to_close_timeout=timedelta(seconds=60)
        )
        downloaded_file = await workflow.execute_activity(
            download_file,
            args=[temp_location, target_url],
            start_to_close_timeout=timedelta(seconds=60),
        )
        decompressed_file = await workflow.execute_activity(
            decompress_file,
            args=[temp_location, downloaded_file],
            start_to_close_timeout=timedelta(seconds=60),
        )
        await workflow.execute_activity(
            clickhouse_create_table_geoip,
            input["ip_family"],
            start_to_close_timeout=timedelta(seconds=60),
        )
        records = await workflow.execute_activity(
            clickhouse_import_geoip,
            args=[input["ip_family"], decompressed_file],
            start_to_close_timeout=timedelta(seconds=60),
        )
        await workflow.execute_activity(
            delete_temp_location,
            temp_location,
            start_to_close_timeout=timedelta(seconds=60),
        )

        return records
