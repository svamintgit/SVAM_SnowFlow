-- Load Queens Arrests data into the queens_arrests table
COPY INTO queens_arrests
FROM @ext_arrests_stage/Queens_Arrests.csv
FILE_FORMAT = csv_ff;