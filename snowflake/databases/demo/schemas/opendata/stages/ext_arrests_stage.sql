-- CREATE STAGE IF NOT EXISTS ext_arrests_stage
--   URL = 'azure://mysfdemo.blob.core.windows.net/mysfcontainer/'
--   STORAGE_INTEGRATION = bobsled_azure_integration
--   FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
--   COMMENT = 'Stage for Azure container mysfcontainer';
