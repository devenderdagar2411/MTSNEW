{{ config(
    materialized='incremental',
    database=var('silver_database'),
    schema=var('silver_schema'),
    alias='T_DIM_CATEGORY_GROUP_NAME',
    unique_key='GROUP_ID'
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
        CAST(TRIM(N55GP) AS NUMBER(5,0)) AS GROUP_ID,
        CAST(TRIM(N55NAME) AS VARCHAR(40)) AS GROUP_NAME,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT
    FROM {{ source('bronze_data', 'T_BRZ_CATEGORY_GRP_NAME_CTGPNM') }}
    where ENTRY_TIMESTAMP > (select max_loaded_ts from latest_loaded)
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by GROUP_ID
            order by ENTRY_TIMESTAMP desc
        ) as rn
    from source_data
),

final_data as (
    select
        GROUP_ID,
        GROUP_NAME,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        -- Generate checksum using key + data fields with MD5
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(GROUP_NAME), '')
        )) AS RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    from ranked_data
    where rn = 1
)

select
    CAST(GROUP_ID AS NUMBER(5)) as GROUP_ID,                                     -- NUMBER(5)
    CAST(GROUP_NAME AS VARCHAR(40)) as GROUP_NAME,                               -- VARCHAR(40)
    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,                        -- VARCHAR(100)
    CAST(SOURCE_FILE_NAME AS VARCHAR(200)) as SOURCE_FILE_NAME,                  -- VARCHAR(200)
    CAST(BATCH_ID AS VARCHAR(50)) as BATCH_ID,                                   -- VARCHAR(50)
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,          -- VARCHAR(64)
    CAST(ETL_VERSION AS VARCHAR(20)) as ETL_VERSION,                            -- VARCHAR(20)
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,                    -- TIMESTAMP_NTZ
    CAST(INGESTION_DT AS DATE) as INGESTION_DT                                   -- DATE
from final_data