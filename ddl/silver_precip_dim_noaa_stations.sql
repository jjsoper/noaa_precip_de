CREATE TABLE silver_precip.dim_noaa_stations (
  _record_id STRING OPTIONS (description = 'Unique identifier for the record'),
  noaa_station_id STRING OPTIONS (description = 'NOAA station ID'),
  noaa_station_name STRING OPTIONS (description = 'Name of the NOAA station'),
  noaa_station_latitude FLOAT64 OPTIONS (description = 'Latitude of the NOAA station'),
  noaa_station_longitude FLOAT64 OPTIONS (description = 'Longitude of the NOAA station'),
  noaa_station_geopoint GEOGRAPHY OPTIONS (description = 'Geographical point of the NOAA station'),
  noaa_station_elevation_m FLOAT64 OPTIONS (description = 'Elevation of the NOAA station in meters'),
  _trace_id STRING OPTIONS (description = 'ID of the trace that triggered the job'),
  _record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS (description = 'Record creation timestamp'),
  _record_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS (description = 'Record update timestamp'),
) PARTITION BY DATE(_record_created_at);