CREATE TABLE silver_precip.fact_noaa_station_precipitation (
  _record_id STRING OPTIONS (description = 'Unique identifier for the record'),
  noaa_station_id STRING OPTIONS (description = 'NOAA station ID'),
  noaa_record_id STRING OPTIONS (description = 'NOAA record ID'),
  hourly_precip_timestamp TIMESTAMP OPTIONS (description = 'Timestamp of the hourly precipitation observation'),
  hourly_precip_value FLOAT64 OPTIONS (description = 'Precipitation value'),
  hourly_precip_unit STRING OPTIONS (description = 'Unit of the precipitation value'),
  hourly_precip_qc STRING OPTIONS (description = 'Quality control flag for the precipitation value'),
  _job_id STRING OPTIONS (description = 'ID of the data ingestion job'),
  _trace_id STRING OPTIONS (description = 'ID of the trace that triggered the job'),
  _record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS (description = 'Record creation timestamp'),
  _record_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS (description = 'Record update timestamp'),
) PARTITION BY DATE(hourly_precip_timestamp);
