/*create a smaller table*/
CREATE TABLE StormEvent_details_reduced AS (SELECT BEGIN_YEARMONTH, BEGIN_DAY, END_YEARMONTH, END_DAY, EVENT_ID, STATE, EVENT_TYPE, INJURIES_DIRECT, INJURIES_INDIRECT, DEATHS_DIRECT, DEATHS_INDIRECT, DAMAGE_PROPERTY, DAMAGE_CROPS, BEGIN_LAT, BEGIN_LON, END_LAT, END_LON, DATA_SOURCE FROM StormEvents_details);

/*add up all death per state and year and create a new table with output values*/
CREATE TABLE StormEvent_details_deathsperstate as (SELECT STATE, (SUM(CAST(DEATHS_DIRECT AS INT)) AS DEATHS), (SUBSTRING(BEGIN_YEARMONTH, 1, 4) AS YEAR) FROM StormEvent_details_reduced GROUP BY STATE, (SUBSTRING(BEGIN_YEARMONTH, 1, 4))); 

/*add up all injuies per state and year adn create a new table with output values*/
CREATE TABLE StormEvent_details_injuriesperstate as (SELECT STATE, (SUM(CAST(INJURIES_DIRECT AS INT)) AS INJURIES), (SUBSTRING(BEGIN_YEARMONTH, 1, 4) AS YEAR) FROM StormEvent_details_reduced GROUP BY STATE, (SUBSTRING(BEGIN_YEARMONTH, 1, 4))) 

/*add up all damages cost per state and year and create a new table with output values*/
CREATE TABLE StormEvent_details_costperstate as (SELECT STATE, (CAST(SUBSTRING(DAMAGE_PROPERTY, 1, LEN(DAMAGE_PROPERTY)-1)) AS INT) FROM StormEvent_details_reduce GROUP BY STATE);




