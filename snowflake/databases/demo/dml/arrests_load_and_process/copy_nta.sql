-- Load NTA data into the nta table
COPY INTO nta
FROM @ext_arrests_stage/nynta.csv
FILE_FORMAT = csv_ff;
