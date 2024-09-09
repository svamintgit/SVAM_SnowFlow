GRANT USAGE ON SCHEMA demo.opendata TO ROLE analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA demo.opendata TO ROLE analyst;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA demo.opendata TO ROLE engineer;
GRANT USAGE ON SCHEMA demo_dev.opendata TO ROLE engineer;
