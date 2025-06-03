{{ config(
    materialized = 'incremental',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_CATEGORY_GROUP',
    unique_key = 'CATEGORY_GROUP_KEY'
) }}

with source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        CAST(TRIM(M55GP) AS NUMBER(10, 0)) AS M55GP,
        CAST(TRIM(M55CTCD) AS NUMBER(10, 0)) AS M55CTCD,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        INGESTION_DTTM,
        INGESTION_DT
    from {{ source('bronze_data', 'T_BRZ_CATEGORY_GROUP_CTGP') }}

    {% if is_incremental() %}
    where ENTRY_TIMESTAMP = (
        select coalesce(max(INGESTION_DTTM), '1900-01-01') from {{ this }}
    )
    {% endif %}
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by M55CTCD
            order by ENTRY_TIMESTAMP desc
        ) as rn
    from source_data
),

final_data as (
    select
        *,
        MD5(CONCAT_WS('|',
            COALESCE(TO_VARCHAR(M55CTCD), '')
        )) AS RECORD_CHECKSUM_HASH,
        ABS(HASH(M55GP || '|' || M55CTCD)) as CATEGORY_GROUP_KEY
    from ranked_data
    where rn = 1
)

select
    CAST(CATEGORY_GROUP_KEY AS NUMBER(20)) as CATEGORY_GROUP_KEY,     
    CAST(M55GP AS NUMBER(3)) as GROUP_ID,                                   
    CAST(M55CTCD AS NUMBER(3)) as CATEGORY_ID,                              
    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,                   
    CAST(SOURCE_FILE_NAME AS VARCHAR(200)) as SOURCE_FILE_NAME,             
    CAST(BATCH_ID AS VARCHAR(50)) as BATCH_ID,                               
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,      
    CAST(ETL_VERSION AS VARCHAR(20)) as ETL_VERSION,                        
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,                  
    CAST(INGESTION_DT AS DATE) as INGESTION_DT  
from final_data
