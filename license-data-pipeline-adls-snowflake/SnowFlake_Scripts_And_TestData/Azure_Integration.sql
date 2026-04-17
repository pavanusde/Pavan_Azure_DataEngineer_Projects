CREATE OR REPLACE STORAGE INTEGRATION azure_adls_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '162123c0-f37b-4275-b5fd-e31058379209'
  STORAGE_ALLOWED_LOCATIONS = ('azure://licdatadldev01.blob.core.windows.net/rawdata/raw/');

  --Step 2 Validate 
  DESC INTEGRATION azure_adls_int;