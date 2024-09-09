-- Load NTA data into the nta table
COPY INTO nta
FROM @arrests_stage/nynta.csv
FILE_FORMAT = csv_ff;
