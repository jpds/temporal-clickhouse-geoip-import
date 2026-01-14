# Temporal ClickHouse GeoIP Import

Temporal workflow for importing the GeoIP datasets found here on GitHub into a
ClickHouse database:

- https://github.com/sapics/ip-location-db

This workflow was created to have a reliable way of enriching records in a
ClickHouse database without having the ClickHouse server itself call out to
the Internet.
