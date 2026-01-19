#!/usr/bin/env python3

import asyncio

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.envconfig import ClientConfigProfile
from temporalio.worker import Worker


from activities import (
    create_temp_location,
    clickhouse_create_geoip_records_table,
    clickhouse_create_geoip_shared_table,
    clickhouse_insert_geoip_records,
    clickhouse_insert_geoip_shared_table_records,
    download_file,
    decompress_file,
    read_geoip_dataset_version,
)
from workflows import (
    ClickHouseGeoIPImport,
    ClickHouseGeoIPInsert,
    ClickHouseGeoIPSharedTableInsert,
)


async def main():
    default_profile = ClientConfigProfile.load()
    connect_config = default_profile.to_client_connect_config()
    client = await Client.connect(**connect_config)

    print(
        f"Client connected to {client.service_client.config.target_host} in namespace '{client.namespace}'"
    )

    worker = Worker(
        client,
        task_queue="clickhouse-geoip-import-queue",
        workflows=[
            ClickHouseGeoIPImport,
            ClickHouseGeoIPInsert,
            ClickHouseGeoIPSharedTableInsert,
        ],
        activities=[
            create_temp_location,
            download_file,
            decompress_file,
            clickhouse_create_geoip_records_table,
            clickhouse_create_geoip_shared_table,
            clickhouse_insert_geoip_records,
            clickhouse_insert_geoip_shared_table_records,
            read_geoip_dataset_version,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
