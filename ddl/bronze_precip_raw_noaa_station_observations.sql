CREATE TABLE bronze_precip.raw_noaa_station_observations (
  _record_id STRING OPTIONS (description = 'Unique identifier for the record'),
  raw_response_json JSON OPTIONS (description = 'Raw JSON response from the NOAA API'),
  _job_id STRING OPTIONS (description = 'ID of the data ingestion job'),
  _event_id STRING OPTIONS (description = 'ID of the event that triggered the job'),
  _record_created_at TIMESTAMP OPTIONS (description = 'Record creation timestamp'),
  _record_updated_at TIMESTAMP OPTIONS (description = 'Record update timestamp'),
) PARTITION BY DATE(_record_created_at);