-- Creating Database demo and demo_dev
CREATE DATABASE demo;
CREATE DATABASE demo_dev;

-- Creating Schema opendata
CREATE SCHEMA opendata;

USE DATABASE demo_dev;
USE SCHEMA opendata;

-- Data for data from Arrests dataset
CREATE TABLE opendata.arrests (
    ARREST_KEY INT,
    ARREST_DATE DATE,
    PD_CD INT,
    PD_DESC STRING,
    KY_CD INT,
    OFNS_DESC STRING,
    LAW_CODE STRING,
    LAW_CAT_CD CHAR(1),
    ARREST_BORO CHAR(1),
    ARREST_PRECINCT INT,
    JURISDICTION_CODE INT,
    AGE_GROUP STRING,
    PERP_SEX CHAR(1),
    PERP_RACE STRING,
    X_COORD_CD INT,
    Y_COORD_CD INT,
    Latitude FLOAT,
    Longitude FLOAT,
    New_Georeferenced_Column GEOGRAPHY
);