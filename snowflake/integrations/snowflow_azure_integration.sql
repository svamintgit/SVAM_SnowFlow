CREATE STORAGE INTEGRATION IF NOT EXISTS bobsled_azure_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'AZURE'
ENABLED = TRUE
AZURE_TENANT_ID = 'd968c2ec-2b1c-425a-bf65-99891de8a7f8'
STORAGE_ALLOWED_LOCATIONS = ('azure://mysnowfdemo.blob.core.windows.net/mysfcontainer')
COMMENT = 'Azure Storage Integration for SnowFlow';
