-- Load Bronx Arrests data into the bronx_arrests table
COPY INTO bronx_arrests
FROM @ext_arrests_stage/Bronx_Arrests.csv
FILE_FORMAT = csv_ff;
