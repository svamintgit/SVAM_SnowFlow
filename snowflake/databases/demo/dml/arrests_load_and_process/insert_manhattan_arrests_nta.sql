-- Manhattan arrests with NTA
INSERT INTO manhattan_arrests_with_nta
SELECT 
    abr.ARREST_KEY, 
    abr.ARREST_DATE, 
    abr.AGE_GROUP, 
    abr.PERP_SEX, 
    abr.PERP_RACE, 
    abr.OFNS_DESC, 
    abr.New_Georeferenced_Column, 
    n.NTAName, 
    n.BoroName
FROM manhattan_arrests abr
JOIN nta n
ON ST_CONTAINS(n.the_geom, abr.New_Georeferenced_Column);