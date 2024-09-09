-- Create the NTA table
CREATE TABLE IF NOT EXISTS nta (
    the_geom GEOGRAPHY, 
    BoroCode INT,
    BoroName STRING,
    CountyFIPS STRING,
    NTACode STRING,
    NTAName STRING,
    Shape_Leng FLOAT,
    Shape_Area FLOAT
);
