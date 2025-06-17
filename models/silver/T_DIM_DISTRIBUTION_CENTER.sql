{{ config(
    materialized = 'incremental',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_DISTRIBUTION_CENTER',
    unique_key = 'STORE_ID'
) }}

with latest_loaded as (
    {% if is_incremental() %}
        select coalesce(max(ENTRY_TIMESTAMP), '1900-01-01'::timestamp) as max_loaded_ts
        from {{ source('bronze_data', 't_brz_distribution_center_stdc') }}
    {% else %}
        select '1900-01-01'::timestamp as max_loaded_ts
    {% endif %}
),

source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        M5DC,                -- DISTRIBUTION_CENTER_ID
        M5STORE,             -- STORE_ID
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        ETL_VERSION
    from {{ source('bronze_data', 't_brz_distribution_center_stdc') }}
    where ENTRY_TIMESTAMP = (select max_loaded_ts from latest_loaded)
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by M5STORE
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
        md5(concat_ws('|', coalesce(trim(M5DC), ''))) as RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    from ranked_data
    where rn = 1
)


select
    CAST(M5DC AS NUMBER(3)) as DISTRIBUTION_CENTER_ID,                                   
    CAST(M5STORE AS number(38,0)) as STORE_ID,                              
    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,                   
    CAST(SOURCE_FILE_NAME AS VARCHAR(200)) as SOURCE_FILE_NAME,             
    CAST(BATCH_ID AS VARCHAR(50)) as BATCH_ID,                               
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,      
    CAST(ETL_VERSION AS VARCHAR(20)) as ETL_VERSION,                        
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,                  
    CAST(INGESTION_DT AS DATE) as INGESTION_DT                           
from final_data
