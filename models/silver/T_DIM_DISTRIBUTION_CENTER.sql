{{ config(
    materialized = 'incremental',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_DISTRIBUTION_CENTER',
    unique_key = 'DISTRIBUTION_CENTER_ID'
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
        -- Surrogate Key
        cast(abs(hash(M5DC || '|' || coalesce(to_varchar(M5STORE), ''))) as bigint) as DISTRIBUTION_CENTER_KEY,

        -- Business Keys
        cast(M5DC as integer) as DISTRIBUTION_CENTER_ID,
        cast(M5STORE as integer) as STORE_ID,

        -- Audit Fields
        cast(SOURCE_SYSTEM as varchar(100)) as SOURCE_SYSTEM,
        cast(SOURCE_FILE_NAME as varchar(200)) as SOURCE_FILE_NAME,
        cast(BATCH_ID as varchar(50)) as BATCH_ID,
        md5(concat_ws('|', coalesce(trim(M5DC), ''))) as RECORD_CHECKSUM_HASH,
        cast(ETL_VERSION as varchar(20)) as ETL_VERSION,
        cast(current_timestamp as timestamp_ntz) as INGESTION_DTTM,
        cast(current_date as date) as INGESTION_DT
    from ranked_data
    where rn = 1
)

select * from final_data
