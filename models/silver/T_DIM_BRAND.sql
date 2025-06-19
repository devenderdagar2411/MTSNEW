{{ config(
    materialized='incremental',
    database=var('silver_database'),
    schema=var('silver_schema'),
    alias = 'T_DIM_BRAND',
    unique_key = 'BRAND_ID'
) }}

with latest_loaded as (
    {% if is_incremental() %}
        select coalesce(max(ENTRY_TIMESTAMP), '1899-12-31T00:00:00Z') as max_loaded_ts
        from {{ this }}
    {% endif %}
),

source_data as (
    select
         TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
       CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
       CAST(TRIM(B99BSCD) AS NUMBER(3,0)) AS BRAND_ID,  
        CAST(TRIM(B99NAME) AS VARCHAR(100)) AS BRAND_NAME,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT
    from {{ source('bronze_data', 't_brz_brand_inbrnd') }}
    where ENTRY_TIMESTAMP = (select max_loaded_ts from latest_loaded)
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by BRAND_ID 
            order by ENTRY_TIMESTAMP desc
        ) as rn
    from source_data
),

final_data as (
    select
    
       BRAND_ID,
       BRAND_NAME,
       SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
       BATCH_ID,
        md5(concat_ws('|', coalesce(trim(BRAND_NAME), ''))) as RECORD_CHECKSUM_HASH,
      ETL_VERSION,
         CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    from ranked_data
    where rn = 1
)

select
    CAST(BRAND_ID AS NUMBER(3)) as BRAND_ID,                                   
    CAST(BRAND_NAME AS VARCHAR(100)) as BRAND_NAME,                              
    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,                   
    CAST(SOURCE_FILE_NAME AS VARCHAR(200)) as SOURCE_FILE_NAME,             
    CAST(BATCH_ID AS VARCHAR(50)) as BATCH_ID,                               
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,      
    CAST(ETL_VERSION AS VARCHAR(20)) as ETL_VERSION,                        
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,                  
    CAST(INGESTION_DT AS DATE) as INGESTION_DT                           
from final_data
