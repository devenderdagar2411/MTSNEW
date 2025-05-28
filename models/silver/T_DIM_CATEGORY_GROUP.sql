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
        OPERATION,
        CAST(TRIM(M55GP) AS NUMBER(10, 0)) AS M55GP,
        CAST(TRIM(M55CTCD) AS NUMBER(10, 0)) AS M55CTCD,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT
    from {{ source('bronze_data', 'T_BRZ_CATEGORY_GROUP_CTGP') }}

    {% if is_incremental() %}
    where INGESTION_DTTM > (
        select coalesce(max(INGESTION_DTTM), '1900-01-01') from {{ this }}
    )
    {% endif %}
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
        *,
        MD5(CONCAT_WS('|',
            COALESCE(TO_VARCHAR(M55GP), ''),
            COALESCE(TO_VARCHAR(M55CTCD), '')
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
