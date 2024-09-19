CREATE STAGE IF NOT EXISTS ext_arrests_stage
  URL = 'azure://mysnowfdemo.blob.core.windows.net/mysfcontainer'
  STORAGE_INTEGRATION = bobsled_azure_integration
  FILE_FORMAT = csv_ff
  COMMENT = 'Stage for Azure container mysfcontainer';
