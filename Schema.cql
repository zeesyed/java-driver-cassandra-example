#keyspace
   CREATE KEYSPACE TESTFLIGHT01
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
  };
  
## Table
  CREATE TABLE flights (
ID int PRIMARY KEY,
YEAR int,      	  	
DAY_OF_MONTH int,
FL_DATE timestamp,
AIRLINE_ID int,
CARRIER varchar,
FL_NUM int,
ORIGIN_AIRPORT_ID int,
ORIGIN varchar,
ORIGIN_CITY_NAME varchar,
ORIGIN_STATE_ABR varchar,
DEST varchar,
DEST_CITY_NAME varchar,
DEST_STATE_ABR varchar,
DEP_TIME timestamp,
ARR_TIME timestamp,
ACTUAL_ELAPSED_TIME timestamp,
AIR_TIME timestamp,
DISTANCE int);

## Create and populate a Cassandra table designed to list all flights leaving a particular airport, sorted by time. 
## Assumption 1: time is time when the flight is leaving.
## Assumption 2: Required sort order is ASCending.
## Optionally YEAR could be added to partition key to address 'unbounded row growth'.
  CREATE TABLE flightsbyairport (
ID int,
YEAR int,      	  	
DAY_OF_MONTH int,
FL_DATE timestamp,
AIRLINE_ID int,
CARRIER varchar,
FL_NUM int,
ORIGIN_AIRPORT_ID int,
ORIGIN varchar,
ORIGIN_CITY_NAME varchar,
ORIGIN_STATE_ABR varchar,
DEST varchar,
DEST_CITY_NAME varchar,
DEST_STATE_ABR varchar,
DEP_TIME timestamp,
ARR_TIME timestamp,
ACTUAL_ELAPSED_TIME int,
AIR_TIME int,
DISTANCE int,
PRIMARY KEY ((ORIGIN_AIRPORT_ID), DEP_TIME)
);