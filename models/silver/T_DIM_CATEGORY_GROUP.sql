{{ config(
    materialized = 'table',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_CATEGORY_GROUP'
) }}

with source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        M55GP,               -- GROUP_ID (Key field)
        M55CTCD,             -- CATEGORY_ID (Data field)
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT
    from RAW_DATA.BRONZE_SALES.T_BRZ_CATEGORY_GROUP_CTGP
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by M55GP
            order by ENTRY_TIMESTAMP desc, SEQUENCE_NUMBER desc
        ) as rn
    from source_data
),

final_data as (
    select
        M55GP,
        M55CTCD,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        -- Generate checksum using key + data fields with MD5
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(M55GP), ''),
            COALESCE(TRIM(M55CTCD), '')
        )) AS RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT,
        -- Generate surrogate key using HASH (ensure positive BIGINT)
        ABS(HASH(M55GP || '|' || M55CTCD)) as CATEGORY_GROUP_KEY
    from ranked_data
    where rn = 1
)

select
    CAST(CATEGORY_GROUP_KEY AS BIGINT) as CATEGORY_GROUP_KEY,                 -- BIGINT(19)
    CAST(M55GP AS INTEGER) as GROUP_ID,                                       -- INTEGER(10)
    CAST(M55CTCD AS INTEGER) as CATEGORY_ID,                                  -- INTEGER(10)
    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,                     -- VARCHAR(100)
    CAST(SOURCE_FILE_NAME AS VARCHAR(200)) as SOURCE_FILE_NAME,              -- VARCHAR(200)
    CAST(BATCH_ID AS VARCHAR(50)) as BATCH_ID,                                -- VARCHAR(50)
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,       -- VARCHAR(64)
    CAST(ETL_VERSION AS VARCHAR(20)) as ETL_VERSION,                          -- VARCHAR(20)
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,                  -- TIMESTAMP_NTZ
    CAST(INGESTION_DT AS DATE) as INGESTION_DT                                -- DATE
from final_data
