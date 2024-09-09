-- Create the Staten Island arrests table
CREATE OR REPLACE TABLE statenisland_arrests (
    ARREST_KEY INT,
    ARREST_DATE DATE,
    PD_CD INT,
    PD_DESC STRING,
    KY_CD INT,
    OFNS_DESC STRING,
    LAW_CODE STRING,
    LAW_CAT_CD VARCHAR(10),
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
