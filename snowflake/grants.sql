-- Creating role engineer
CREATE ROLE engineer;

-- Creating rol analyst
CREATE ROLE analyst;

-- Setting role hierarchy
GRANT ROLE engineer TO ROLE sysadmin;
GRANT ROLE analyst TO ROLE engineer;

-- Creating 3 warehouses
CREATE WAREHOUSE load WITH WAREHOUSE_SIZE = 'XSMALL';
CREATE WAREHOUSE transform WITH WAREHOUSE_SIZE = 'XSMALL';
CREATE WAREHOUSE analyze WITH WAREHOUSE_SIZE = 'XSMALL';

-- Assigning warehouses to roles
GRANT OPERATE ON WAREHOUSE load TO ROLE engineer;
GRANT OPERATE ON WAREHOUSE transform TO ROLE engineer;
GRANT OPERATE ON WAREHOUSE analyze TO ROLE analyst;


