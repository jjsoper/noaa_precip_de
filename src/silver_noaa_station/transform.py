import uuid

from src.managers.bigquery_manager import BigQueryManager


def filtered_bronze_snippet(target_table: str, source_table: str) -> str:
    return f"""
        (
            SELECT b._record_id,
                b._record_updated_at,
                b.raw_response_json
            FROM {source_table} b
            WHERE b._record_updated_at > (
                    SELECT COALESCE(
                            MAX(s._record_updated_at),
                            TIMESTAMP("1970-01-01")
                        )
                    FROM {target_table} s
                )
        )
    """


# TODO: clean this up, it's not functioning
# def transform_dim_noaa_stations(
#     target_table,
#     source_table,
#     job_id: str,
#     trace_id: str,
#     bigquery_manager: BigQueryManager,
# ):

#     QUERY = f"""
#         MERGE {target_table} T USING (
#             WITH
#             filtered_bronze AS {filtered_bronze_snippet(target_table, source_table)},
#             parsed AS (
#                 SELECT _record_id,
#                     _record_updated_at,
#                     JSON_VALUE(raw_response_json.properties.stationId) AS noaa_station_id,
#                     JSON_VALUE(raw_response_json.properties.stationName) AS noaa_station_name,
#                     SAFE_CAST(JSON_VALUE(raw_response_json.geometry.coordinates[0]) AS FLOAT64) AS noaa_station_longitude,
#                     SAFE_CAST(JSON_VALUE(raw_response_json.geometry.coordinates[1]) AS FLOAT64) AS noaa_station_latitude,
#                     ST_GEOGPOINT(
#                         SAFE_CAST(JSON_VALUE(raw_response_json.geometry.coordinates[0]) AS FLOAT64),
#                         SAFE_CAST(JSON_VALUE(raw_response_json.geometry.coordinates[1]) AS FLOAT64))
#                     AS noaa_station_geopoint
#                     SAFE_CAST(JSON_VALUE(raw_response_json.properties.elevation.value) AS FLOAT64) AS noaa_station_elevation_m,
#                 FROM filtered_bronze
#             ),
#             deduplicated AS (
#                 SELECT *
#                 EXCEPT(rn)
#                 FROM (
#                         SELECT *,
#                             ROW_NUMBER() OVER (
#                                 PARTITION BY noaa_record_id
#                                 ORDER BY _record_updated_at DESC
#                             ) AS rn
#                         FROM parsed
#                     )
#                 WHERE rn = 1
#             )
#             SELECT *,
#                 GENERATE_UUID() AS _job_id,
#                 GENERATE_UUID() AS _trace_id
#             FROM deduplicated
#         ) S ON T.noaa_record_id = S.noaa_record_id-- Matching on the unique NOAA ID
#         WHEN MATCHED THEN
#         UPDATE
#         SET
#             T._record_id = S._record_id,
#             T.noaa_station_id = S.noaa_station_id,
#             T.noaa_station_name = S.noaa_station_name,
#             T.noaa_station_longitude = S.noaa_station_longitude,
#             T.noaa_station_latitude = S.noaa_station_latitude,
#             T.noaa_station_geopoint = S.noaa_station_geopoint,
#             T.noaa_station_elevation_m = S.noaa_station_elevation_m,
#             T._job_id = "{job_id}",
#             T._trace_id = "{trace_id}"
#         WHEN NOT MATCHED THEN
#         INSERT (
#                 _record_id,
#                 noaa_station_id,
#                 noaa_station_name,
#                 noaa_station_longitude,
#                 noaa_station_latitude,
#                 noaa_station_geopoint,
#                 noaa_station_elevation_m,
#                 _job_id,
#                 _trace_id
#             )
#         VALUES (
#                 S._record_id,
#                 S.noaa_station_id,
#                 S.noaa_station_name,
#                 S.noaa_station_longitude,
#                 S.noaa_station_latitude,
#                 S.noaa_station_geopoint,
#                 S.noaa_station_elevation_m,
#                 "{job_id}",
#                 "{trace_id}"
#             );

#     """

#     query_job = bigquery_manager.query(QUERY)
#     return query_job


def transform_fact_noaa_station_observations(
    target_table,
    source_table,
    job_id: str,
    trace_id: str,
    bigquery_manager: BigQueryManager,
):

    QUERY = f"""
        MERGE {target_table} T USING (
            WITH {filtered_bronze_snippet(target_table, source_table)},
            parsed AS (
                SELECT _record_id,
                    _record_updated_at,
                    JSON_VALUE(raw_response_json.properties.stationId) AS noaa_station_id,
                    JSON_VALUE(raw_response_json.id) AS noaa_record_id,
                    TIMESTAMP(
                        JSON_VALUE(raw_response_json.properties.timestamp)
                    ) AS hourly_precip_timestamp,
                    SAFE_CAST(
                        JSON_VALUE(
                            raw_response_json.properties.precipitationLastHour.value
                        ) AS FLOAT64
                    ) AS hourly_precip_value,
                    REPLACE(
                        JSON_VALUE(
                            raw_response_json.properties.precipitationLastHour.unitCode
                        ),
                        'wmoUnit:',
                        ''
                    ) AS hourly_precip_unit,
                    JSON_VALUE(
                        raw_response_json.properties.precipitationLastHour.qualityControl
                    ) AS hourly_precip_qc
                FROM filtered_bronze
            ),
            deduplicated AS (
                SELECT *
                EXCEPT(rn)
                FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY noaa_record_id
                                ORDER BY _record_updated_at DESC
                            ) AS rn
                        FROM parsed
                    )
                WHERE rn = 1
                    AND EXTRACT(
                        MINUTE
                        FROM hourly_precip_timestamp
                    ) = 54
            )
            SELECT *,
                GENERATE_UUID() AS _job_id,
                GENERATE_UUID() AS _trace_id
            FROM deduplicated
        ) S ON T.noaa_record_id = S.noaa_record_id-- Matching on the unique NOAA ID
        WHEN MATCHED THEN
        UPDATE
        SET 
            T._record_id = S._record_id,
            T.noaa_station_id = S.noaa_station_id,
            T.hourly_precip_timestamp = S.hourly_precip_timestamp,
            T.hourly_precip_value = S.hourly_precip_value,
            T.hourly_precip_unit = S.hourly_precip_unit,
            T.hourly_precip_qc = S.hourly_precip_qc,
            T._job_id = "{job_id}",
            T._trace_id = "{trace_id}"
        WHEN NOT MATCHED THEN
        INSERT (
                _record_id,
                noaa_station_id,
                noaa_record_id,
                hourly_precip_timestamp,
                hourly_precip_value,
                hourly_precip_unit,
                hourly_precip_qc,
                _job_id,
                _trace_id
            )
        VALUES (
                S._record_id,
                S.noaa_station_id,
                S.noaa_record_id,
                S.hourly_precip_timestamp,
                S.hourly_precip_value,
                S.hourly_precip_unit,
                S.hourly_precip_qc,
                "{job_id}",
                "{trace_id}"
            );

    """

    query_job = bigquery_manager.query(QUERY)
    return query_job
