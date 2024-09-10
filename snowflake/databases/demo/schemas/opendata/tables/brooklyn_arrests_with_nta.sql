-- Brooklyn Arrests with NTA
CREATE OR REPLACE TABLE brooklyn_arrests_with_nta (
    ARREST_KEY INT,
    ARREST_DATE DATE,
    AGE_GROUP STRING,
    PERP_SEX CHAR(1),
    PERP_RACE STRING,
    OFNS_DESC STRING,
    New_Georeferenced_Column GEOGRAPHY,
    NTAName STRING,
    BoroName STRING
);