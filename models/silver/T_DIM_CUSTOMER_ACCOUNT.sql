{{ config(
    materialized = 'incremental',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_CUSTOMER_ACCOUNT',
    unique_key = 'ACCOUNT_TYPE'
) }}

with source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        BYACTY,                -- ACCOUNT_TYPE
        BYNAME,                -- ACCOUNT_TYPE_NAME
        BYUSER,                -- LAST_MAINTAINED_USER
        BYCYMD,                -- LAST_MODIFIED_DATE
        BYHMS,                 -- LAST_MAINTAINED_TIME
        BYWKSN,                -- LAST_MAINTAINED_WORKSTATION
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        RECORD_CHECKSUM_HASH,
        ETL_VERSION
    from {{ source('bronze_data', 'T_BRZ_CUSTOMER_ACCOUNT_SACACH') }}
    {% if is_incremental() %}
    --     -- Only pull records newer than the latest already loaded
    where ENTRY_TIMESTAMP > (select coalesce(max(ENTRY_TIMESTAMP), '1899-12-31T00:00:00Z') from {{ this }})
    {% endif %}
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by BYACTY 
            order by ENTRY_TIMESTAMP desc
        ) as rn
    from source_data
),

final_data as (
    select
        BYACTY,
        BYNAME,
        BYUSER,
        BYCYMD,
        BYHMS,
        BYWKSN,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        md5(concat_ws('|', coalesce(trim(BYNAME), ''))) as RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT

    from ranked_data
    where rn = 1
)

select
        
    CAST(BYACTY AS NUMBER(3)) as ACCOUNT_TYPE,                                   
    CAST(BYNAME AS VARCHAR(100)) as ACCOUNT_TYPE_NAME,
    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,                   
    CAST(SOURCE_FILE_NAME AS VARCHAR(200)) as SOURCE_FILE_NAME,             
    CAST(BATCH_ID AS VARCHAR(50)) as BATCH_ID,                               
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,      
    CAST(ETL_VERSION AS VARCHAR(20)) as ETL_VERSION,                        
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,                  
    CAST(INGESTION_DT AS DATE) as INGESTION_DT                           
from final_data