{{
  config(
    materialized = 'table',
    alias = 'dim_saslrm_full'
  )
}}

-- Full load of the customer master data
-- This will be the initial load before implementing SCD Type 2

SELECT
    ROW_NUMBER() OVER (PARTITION BY M0SLRP) as SURROGATE_KEY,
    ENTRY_TIMESTAMP,
    SEQUENCE_NUMBER,
    OPERATION,
    M0SLRP,
    M0NAME,
    M0SPCM,
    SOURCE_SYSTEM,
    SOURCE_FILE_NAME,
    BATCH_ID,
    RECORD_CHECKSUM_HASH,
    ETL_VERSION,
    INGESTION_DTTM,
    INGESTION_DT,
    -- Create hash to track changes specifically in M0NAME and M0SPCM
    MD5(CONCAT(
        COALESCE(M0NAME::VARCHAR, ''), '|',
        COALESCE(M0SPCM::VARCHAR, '')
    )) as TRACKING_HASH,
    CURRENT_TIMESTAMP() as DBT_UPDATED_AT,
    'DBT' as DBT_UPDATED_BY,
    ENTRY_TIMESTAMP as VALID_FROM,
    NULL as VALID_TO,
    TRUE as IS_CURRENT
FROM {{ source('raw_data', 'T_BRZ_SALESREP_MASTER_SASLR_CLONED') }}
-- Take only the latest record for each M0SLRP in the initial load

