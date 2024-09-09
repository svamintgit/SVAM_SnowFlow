-- Queens arrests mapped to NTAs using geospatial functions
CREATE OR REPLACE VIEW queens_arrests_geo_v AS
SELECT 
    abr.ARREST_KEY,
    abr.ARREST_DATE,
    abr.OFNS_DESC,
    abr.PERP_RACE,
    abr.PERP_SEX,
    abr.AGE_GROUP,
    abr.New_Georeferenced_Column, 
    n.NTAName, 
    n.BoroName 
FROM 
    opendata.queens_arrests_with_nta abr
JOIN 
    opendata.nta n
ON 
    ST_CONTAINS(n.the_geom, abr.New_Georeferenced_Column);