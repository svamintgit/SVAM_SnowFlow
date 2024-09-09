-- View combining arrests from all boroughs mapped to NTAs
CREATE OR REPLACE VIEW all_boroughs_arrests_geo_v AS
SELECT 
    'Bronx' AS Borough, 
    ARREST_KEY, ARREST_DATE, OFNS_DESC, PERP_RACE, PERP_SEX, AGE_GROUP, New_Georeferenced_Column, NTAName, BoroName
FROM 
    opendata.bronx_arrests_geo_v
UNION ALL
SELECT 
    'Brooklyn' AS Borough, 
    ARREST_KEY, ARREST_DATE, OFNS_DESC, PERP_RACE, PERP_SEX, AGE_GROUP, New_Georeferenced_Column, NTAName, BoroName
FROM 
    opendata.brooklyn_arrests_geo_v
UNION ALL
SELECT 
    'Manhattan' AS Borough, 
    ARREST_KEY, ARREST_DATE, OFNS_DESC, PERP_RACE, PERP_SEX, AGE_GROUP, New_Georeferenced_Column, NTAName, BoroName
FROM 
    opendata.manhattan_arrests_geo_v
UNION ALL
SELECT 
    'Queens' AS Borough, 
    ARREST_KEY, ARREST_DATE, OFNS_DESC, PERP_RACE, PERP_SEX, AGE_GROUP, New_Georeferenced_Column, NTAName, BoroName
FROM 
    opendata.queens_arrests_geo_v
UNION ALL
SELECT 
    'Staten Island' AS Borough, 
    ARREST_KEY, ARREST_DATE, OFNS_DESC, PERP_RACE, PERP_SEX, AGE_GROUP, New_Georeferenced_Column, NTAName, BoroName
FROM 
    opendata.statenisland_arrests_geo_v;
