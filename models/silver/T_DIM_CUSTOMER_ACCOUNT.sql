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
    -- {% if is_incremental() %}
    --     -- Only pull records newer than the latest already loaded
    --     where ENTRY_TIMESTAMP > (select coalesce(max(ENTRY_TIMESTAMP), '1900-01-01') from {{ this }})
    -- {% endif %}
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by BYACTY 
            order by ENTRY_TIMESTAMP desc, SEQUENCE_NUMBER desc
        ) as rn
    from source_data
),

final_data as (
    select
        -- ACCOUNT_TYPE_KEY (BIGINT → hashed surrogate key)
        cast(abs(hash(BYACTY)) as bigint) as ACCOUNT_TYPE_KEY,

        -- ACCOUNT_TYPE (INTEGER)
        cast(BYACTY as integer) as ACCOUNT_TYPE,

        -- ACCOUNT_TYPE_NAME (VARCHAR(40))
        cast(BYNAME as varchar(40)) as ACCOUNT_TYPE_NAME,

        -- LAST_MAINTAINED_USER (VARCHAR(50))
        cast(BYUSER as varchar(50)) as LAST_MAINTAINED_USER,

        -- LAST_MODIFIED_DATE (DATE), assuming BYCYMD is in format YYYYMMDD
        try_to_date(to_varchar(BYCYMD), 'YYYYMMDD') as LAST_MODIFIED_DATE,

        -- LAST_MAINTAINED_TIME (NUMBER(38,0))
        cast(BYHMS as number(38,0)) as LAST_MAINTAINED_TIME,

        -- LAST_MAINTAINED_WORKSTATION (VARCHAR(20))
        cast(BYWKSN as varchar(20)) as LAST_MAINTAINED_WORKSTATION,

        -- Audit Fields
        cast(SOURCE_SYSTEM as varchar(100)) as SOURCE_SYSTEM,
        cast(SOURCE_FILE_NAME as varchar(200)) as SOURCE_FILE_NAME,
        cast(BATCH_ID as varchar(50)) as BATCH_ID,
        cast(RECORD_CHECKSUM_HASH as varchar(64)) as RECORD_CHECKSUM_HASH,
        cast(ETL_VERSION as varchar(20)) as ETL_VERSION,
        cast(current_timestamp as timestamp_ntz) as INGESTION_DTTM,
        cast(current_date as date) as INGESTION_DT

    from ranked_data
    where rn = 1
)

select * from final_data