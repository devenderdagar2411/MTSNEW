{{ config(
    materialized = 'table',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_DISTRIBUTION_CENTER'
) }}

with source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        M5DC,                -- DISTRIBUTION_CENTER_ID (Data field)
        M5STORE,             -- STORE_ID (Key field)
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT
    from RAW_DATA.BRONZE_SALES.T_BRZ_DISTRIBUTION_CENTER_STDC
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by M5DC
            order by ENTRY_TIMESTAMP desc, SEQUENCE_NUMBER desc
        ) as rn
    from source_data
),

final_data as (
    select
        M5DC,
        M5STORE,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        -- Generate checksum using ONLY data field (M5DC)
        MD5(TO_VARCHAR(M5DC)) as RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT,
        -- Generate surrogate key using key fields (M5DC + STORE_ID)
        ABS(HASH(M5DC || '|' || COALESCE(TO_VARCHAR(M5STORE), ''))) as DISTRIBUTION_CENTER_KEY
    from ranked_data
    where rn = 1
)

select
    CAST(DISTRIBUTION_CENTER_KEY AS BIGINT) as DISTRIBUTION_CENTER_KEY,          -- BIGINT(19), surrogate key
    CAST(M5DC AS INTEGER) as DISTRIBUTION_CENTER_ID,                             -- INTEGER(10), not nullable
    CAST(M5STORE AS INTEGER) as STORE_ID,                                        -- INTEGER(10), nullable
    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,                        -- VARCHAR(100)
    CAST(SOURCE_FILE_NAME AS VARCHAR(200)) as SOURCE_FILE_NAME,                 -- VARCHAR(200)
    CAST(BATCH_ID AS VARCHAR(50)) as BATCH_ID,                                   -- VARCHAR(50)
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,          -- VARCHAR(64)
    CAST(ETL_VERSION AS VARCHAR(20)) as ETL_VERSION,                             -- VARCHAR(20)
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,                     -- TIMESTAMP_NTZ
    CAST(INGESTION_DT AS DATE) as INGESTION_DT                                   -- DATE
from final_data
