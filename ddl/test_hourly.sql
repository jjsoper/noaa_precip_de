CREATE TABLE precip.test_hourly (
  id STRING OPTIONS (description = 'Station ID'),
    timestamp TIMESTAMP OPTIONS (description = 'Timestamp of the reading'),
    precipitation FLOAT64 OPTIONS (description = 'Precipitation amount in mm'),
    location STRING OPTIONS (description = 'Location of the station')
) PARTITION BY DATE(timestamp);