-- Load Manhattan Arrests data into the manhattan_arrests table
COPY INTO manhattan_arrests
FROM @ext_arrests_stage/Manhattan_Arrests.csv
FILE_FORMAT = csv_ff;