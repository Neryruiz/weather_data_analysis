/*create a smaller table*/
CREATE TABLE StormEvent_details_reduced AS (SELECT BEGIN_YEARMONTH, BEGIN_DAY, END_YEARMONTH, END_DAY, EVENT_ID, STATE, EVENT_TYPE, INJURIES_DIRECT, INJURIES_INDIRECT, DEATHS_DIRECT, DEATHS_INDIRECT, DAMAGE_PROPERTY, DAMAGE_CROPS, BEGIN_LAT, BEGIN_LON, END_LAT, END_LON, DATA_SOURCE FROM StormEvents_details);

/*add up all death per state and year and create a new table with output values*/
CREATE TABLE StormEvent_details_deathsperstate as (SELECT STATE, SUM(CAST(DEATHS_DIRECT AS INT)) AS DEATHS, SUBSTRING(BEGIN_YEARMONTH, 1, 4) AS YEAR FROM StormEvent_details_reduced GROUP BY STATE, SUBSTRING(BEGIN_YEARMONTH, 1, 4)); 

/*add up all injuies per state and year adn create a new table with output values*/
CREATE TABLE StormEvent_details_injuriesperstate as (SELECT STATE, SUM(CAST(INJURIES_DIRECT AS INT)) AS INJURIES, SUBSTRING(BEGIN_YEARMONTH, 1, 4) AS YEAR FROM StormEvent_details_reduced GROUP BY STATE, SUBSTRING(BEGIN_YEARMONTH, 1, 4)); 

/*add up all damages cost per state and year and create a new table with output values*/
CREATE TABLE StormEvent_details_costperstate as (SELECT STATE, SUM(CAST(SUBSTRING(DAMAGE_PROPERTY, 1, LENGTH(DAMAGE_PROPERTY)-1) AS INT)) AS DAMAGES, SUBSTRING(BEGIN_YEARMONTH, 1, 4) AS YEAR FROM StormEvent_details_reduced GROUP BY STATE, SUBSTRING(BEGIN_YEARMONTH, 1, 4));

/*count weather events per year and state*/
CREATE TABLE StormEvent_details_counttypeperstate as (SELECT STATE, YEAR, EVENT_TYPE, COUNT(EVENT_TYPE) AS TYPE_COUNT FROM StormEventS_details GROUP BY STATE, YEAR, EVENT_TYPE);

/*count weather events, cost, deaths, and injuries per year and state*/
CREATE TABLE StormEvent_details_typecostperstate as (SELECT STATE, YEAR, SUM(CAST(SUBSTRING(DAMAGE_PROPERTY, 1, LENGTH(DAMAGE_PROPERTY)-1) AS INT)) AS DAMAGES, EVENT_TYPE, COUNT(EVENT_TYPE) AS TYPE_COUNT, SUM(CAST(DEATHS_DIRECT AS INT)) AS DEATHS, SUM(CAST(INJURIES_DIRECT AS INT)) AS INJURIES FROM StormEventS_details GROUP BY STATE, YEAR, EVENT_TYPE);


