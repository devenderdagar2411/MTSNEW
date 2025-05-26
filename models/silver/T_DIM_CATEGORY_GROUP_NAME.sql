{{ config(
    materialized = 'table',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_CATEGORY_GROUP_NAME'
) }}

with source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        N55GP,               -- GROUP_ID (Key field)
        N55NAME,             -- GROUP_NAME (Data field)
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT
    FROM {{ source('bronze_data', 'T_BRZ_CATEGORY_GRP_NAME_CTGPNM') }}
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by N55GP
            order by ENTRY_TIMESTAMP desc
        ) as rn
    from source_data
),

final_data as (
    select
        N55GP,
        N55NAME,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        -- Generate checksum using key + data fields with MD5
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(N55NAME), '')
        )) AS RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT,
        -- Generate surrogate key using HASH (ensure positive BIGINT)
        ABS(HASH(N55GP || '|' || N55NAME)) as CATEGORY_GROUP_KEY_NAME
    from ranked_data
    where rn = 1
)

select
    CAST(CATEGORY_GROUP_KEY_NAME AS NUMBER(20)) as CATEGORY_GROUP_KEY_NAME,                 -- BIGINT(19)
    CAST(N55GP AS NUMBER(5)) as GROUP_ID,                                       -- INTEGER(10)
    CAST(N55NAME AS VARCHAR(40)) as GROUP_NAME,                                  -- INTEGER(10)
    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,                     -- VARCHAR(100)
    CAST(SOURCE_FILE_NAME AS VARCHAR(200)) as SOURCE_FILE_NAME,              -- VARCHAR(200)
    CAST(BATCH_ID AS VARCHAR(50)) as BATCH_ID,                                -- VARCHAR(50)
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,       -- VARCHAR(64)
    CAST(ETL_VERSION AS VARCHAR(20)) as ETL_VERSION,                          -- VARCHAR(20)
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,                  -- TIMESTAMP_NTZ
    CAST(INGESTION_DT AS DATE) as INGESTION_DT                                -- DATE
from final_data
