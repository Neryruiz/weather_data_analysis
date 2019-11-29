CREATE EXTERNAL TABLE StormEvents_details 
(BEGIN_YEARMONTH STRING NOT NULL, 
BEGIN_DAY STRING NOT NULL, 
BEGIN_TIME STRING NOT NULL, 
END_YEARMONTH STRING NOT NULL, 
END_DAY STRING NOT NULL, 
END_TIME STRING NOT NULL, 
EPISODE_ID STRING NOT NULL, 
EVENT_ID STRING NOT NULL, 
STATE STRING NOT NULL, 
STATE_FIPS STRING NOT NULL, 
YEAR STRING NOT NULL, 
MONTH_NAME STRING NOT NULL, 
EVENT_TYPE STRING NOT NULL, 
CZ_TYPE STRING NOT NULL, 
CZ_FIPS STRING NOT NULL, 
CZ_NAME STRING NOT NULL, 
WFO STRING NOT NULL, 
BEGIN_DATE_TIME STRING NOT NULL, 
INJURIES_DIRECT STRING NOT NULL, 
INJURIES_INDIRECT STRING NOT NULL, 
DEATHS_DIRECT STRING NOT NULL, 
DEATHS_INDIRECT STRING NOT NULL, 
DAMAGE_PROPERTY STRING NOT NULL, 
DAMAGE_CROPS STRING NOT NULL, 
SOURCE STRING NOT NULL, 
MAGNITUDE STRING NOT NULL, 
MAGNITUDE_TYPE STRING NOT NULL, 
FLOOD_CAUSE STRING NOT NULL, 
CATEGORY STRING NOT NULL, 
TOR_F_SCALE STRING NOT NULL, 
TOR_LENGTH STRING NOT NULL, 
TOR_WIDTH STRING NOT NULL, 
TOR_OTHER_WFO STRING NOT NULL, 
TOR_OTHER_CZ_STATE STRING NOT NULL, 
TOR_OTHER_FIPS STRING NOT NULL, 
TOR_OTHER_CZ_NAME STRING NOT NULL, 
BEGIN_RANGE STRING NOT NULL, 
BEGIN_AZIMUTH STRING NOT NULL, 
BEGIN_LOCATION STRING NOT NULL,
END_RANGE STRING NOT NULL,
END_AZIMUTH STRING NOT NULL,
END_LOCATION STRING NOT NULL,
BEGIN_LAT STRING NOT NULL,
BEGIN_LON STRING NOT NULL,
END_LAT STRING NOT NULL,
END_LON STRING NOT NULL,
EPISODE_NARRATIVE STRING NOT NULL,
EVENT_NARRATIVE STRING NOT NULL,
DATA_SOURCE STRING NOT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'gs://noaa_weather_data/NOAA_Strom_DataSet/StormEvents_details/StormEvents_Details' tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE StormEvents_fatalites 
(FAT_YEARMONTH STRING, 
FAT_DAY STRING, 
FAT_TIME STRING, 
FATALITY_ID STRING, 
EVENT_ID STRING, 
FATALITY_TYPE STRING, 
FATALITY_DATE STRING, 
FATALITY_AGE STRING, 
FATALITY_SEX STRING, 
FATALITY_LOCATION STRING, 
EVENT_YEARMONTH STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'gs://noaa_weather_data/NOAA_Strom_DataSet/StormEvents_details/StormsEvents_Fatality/';

CREATE EXTERNAL TABLE StormEvents_locations
(YEARMONTH STRING, 
EPISODE_ID STRING, 
EVENT_ID STRING, 
LOCATION_INDEX STRING, 
`RANGE` STRING, 
AZIMUTH STRING, 
LOCATION STRING, 
LATITUDE STRING, 
LONGITUDE STRING, 
LAT2 STRING, 
LON2 STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'gs://noaa_weather_data/NOAA_Strom_DataSet/StormEvents_details/StormsEvents_location/';

CREATE EXTERNAL TABLE WeatherEvents_details 
(ZTIME STRING, 
LON STRING, 
LAT STRING, 
`EVENT` STRING, 
MAGNITUDE STRING, 
CITY STRING, 
COUNTY STRING, 
STATE STRING, 
SOURCE STRING, 
WFO STRING, 
`REMARKS` STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'gs://noaa_weather_data/NOAA_Strom_DataSet/WeatherEvents_details/';

CREATE EXTERNAL TABLE WeatherWarnings 
(ISSUEDATE STRING, 
EXPIREDATE STRING, 
ISSUEWFO STRING, 
MESSAGEID STRING, 
MESSAGETYPE STRING, 
WARNINGTYPE STRING, 
POLYGON STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'gs://noaa_weather_data/NOAA_Strom_DataSet/WeatherWarnings/';

CREATE EXTERNAL TABLE Structure 
(ZTIME STRING, 
LON STRING, 
LAT STRING, 
WSR_ID STRING, 
CELL_ID STRING, 
`RANGE` STRING, 
AZIMUTH STRING, 
BASE_HEIGHT STRING, 
TOP_HEIGHT STRING, 
VIL STRING, 
MAX_REFLECT STRING, 
HEIGHT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'gs://noaa_weather_data/NOAA_Strom_DataSet/WeatherStructure/';

CREATE EXTERNAL TABLE lightningStrikes 
(ZDAY STRING, CENTERLON STRING, CENTERLAT STRING, TOTAL_COUNT STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'gs://noaa_weather_data/NOAA_Strom_DataSet/lightingstrike/';

