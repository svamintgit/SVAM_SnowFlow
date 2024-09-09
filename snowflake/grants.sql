-- Setting role hierarchy
GRANT ROLE engineer TO ROLE sysadmin;
GRANT ROLE analyst TO ROLE engineer;

-- Assigning warehouses to roles
GRANT OPERATE ON WAREHOUSE load TO ROLE engineer;
GRANT OPERATE ON WAREHOUSE transform TO ROLE engineer;
GRANT OPERATE ON WAREHOUSE analyze TO ROLE analyst;
