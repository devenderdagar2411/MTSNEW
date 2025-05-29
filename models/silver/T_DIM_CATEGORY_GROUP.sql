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
            order by ENTRY_TIMESTAMP desc, SEQUENCE_NUMBER desc
        ) as rn
    from source_data
),

final_data as (
    select
        *,
        MD5(CONCAT_WS('|',
            COALESCE(TO_VARCHAR(M55GP), '')
        )) AS RECORD_CHECKSUM_HASH,
        ABS(HASH(M55GP || '|' || M55CTCD)) as CATEGORY_GROUP_KEY
    from ranked_data
    where rn = 1
)

select
    CAST(CATEGORY_GROUP_KEY AS NUMBER(20,0)) AS CATEGORY_GROUP_KEY,
    M55GP          AS GROUP_ID,
    M55CTCD        AS CATEGORY_ID,
    SOURCE_SYSTEM,
    SOURCE_FILE_NAME,
    BATCH_ID,
    RECORD_CHECKSUM_HASH,
    ETL_VERSION,
    INGESTION_DTTM,
    INGESTION_DT
from final_data
