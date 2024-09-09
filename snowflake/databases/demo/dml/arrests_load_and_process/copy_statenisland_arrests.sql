-- Load Staten Island Arrests data into the statenisland_arrests table
COPY INTO statenisland_arrests
FROM @arrests_stage/StatenIsland_Arrests.csv
FILE_FORMAT = csv_ff;