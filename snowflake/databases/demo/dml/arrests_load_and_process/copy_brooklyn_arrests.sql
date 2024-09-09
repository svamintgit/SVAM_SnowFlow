-- Load Brooklyn Arrests data into the brooklyn_arrests table
COPY INTO brooklyn_arrests
FROM @arrests_stage/Brooklyn_Arrests.csv
FILE_FORMAT = csv_ff;