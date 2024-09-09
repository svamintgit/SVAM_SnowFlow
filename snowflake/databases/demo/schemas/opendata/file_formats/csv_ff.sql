-- Create a CSV file format to use for the COPY INTO commands
CREATE FILE FORMAT IF NOT EXISTS csv_ff
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;
