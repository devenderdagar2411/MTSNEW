{{ config(
    materialized = 'incremental',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_BRAND',
    unique_key = 'BRAND_ID'
) }}

with source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        B99BSCD,               -- BRAND_ID
        B99NAME,               -- BRAND_NAME
        B99USER,               -- LAST_MODIFIED_USER
        B99CYMD,               -- LAST_MODIFIED_DATE
        B99HMS,                -- LAST_MODIFIED_TIME
        B99WKSN,               -- WORKSTATION_ID
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        ETL_VERSION
    from {{ source('bronze_data', 't_brz_brand_inbrnd') }}
    {% if is_incremental() %}
        -- Only pull records newer than the latest already loaded
        where ENTRY_TIMESTAMP > (select coalesce(max(ENTRY_TIMESTAMP), '1900-01-01') from {{ this }})
    {% endif %}
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by B99BSCD 
            order by ENTRY_TIMESTAMP desc, SEQUENCE_NUMBER desc
        ) as rn
    from source_data
),

final_data as (
    select
        -- BRAND_KEY (BIGINT → hashed surrogate key)
        cast(abs(hash(try_cast(B99BSCD as number(10,0)))) as bigint) as BRAND_KEY,

        -- BRAND_ID (INTEGER(10))
        cast(try_cast(B99BSCD as number(10,0)) as integer) as BRAND_ID,

        -- BRAND_NAME (VARCHAR(100))
        cast(B99NAME as varchar(100)) as BRAND_NAME,

        -- Audit Fields
        cast(SOURCE_SYSTEM as varchar(100)) as SOURCE_SYSTEM,
        cast(SOURCE_FILE_NAME as varchar(200)) as SOURCE_FILE_NAME,
        cast(BATCH_ID as varchar(50)) as BATCH_ID,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(B99BSCD), '')
        )) AS RECORD_CHECKSUM_HASH,
        cast(ETL_VERSION as varchar(20)) as ETL_VERSION,
        cast(current_timestamp as timestamp_ntz) as INGESTION_DTTM,
        cast(current_date as date) as INGESTION_DT
    from ranked_data
    where rn = 1
)

select * from final_data
