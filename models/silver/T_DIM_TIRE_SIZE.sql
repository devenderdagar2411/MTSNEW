{{ config(
    materialized='incremental',
    database=var('silver_database'),
    schema=var('silver_schema'),
    alias='T_DIM_TIRE_SIZE',
    unique_key='TIRE_SIZE_CODE'
) }}

-- Step 1: Incremental Load Strategy
with latest_loaded as (
    {% if is_incremental() %}
        select coalesce(max(ENTRY_TIMESTAMP), '1900-01-01T00:00:00Z') as max_loaded_ts
        from {{ source('bronze_data', 'T_BRZ_TIRE_SIZE_BSOTRM') }}
    {% else %}
        select '1900-01-01T00:00:00Z' as max_loaded_ts
    {% endif %}
),

-- Step 2: Source Extraction with Incremental Filter
source_data as (
    select
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        CAST(TRIM(B9BSCD) AS NUMBER(3,0)) AS TIRE_SIZE_CODE,
        CAST(TRIM(B9NAME) AS VARCHAR(40)) AS TIRE_SIZE_NAME,
        CAST(TRIM(B9USER) AS VARCHAR(50)) AS B9USER,
        CAST(TRIM(B9CYMD) AS VARCHAR(50)) AS B9CYMD,
        CAST(TRIM(B9HMS) AS VARCHAR(50)) AS B9HMS,
        CAST(TRIM(B9WKSN) AS VARCHAR(50)) AS B9WKSN,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT
    from {{ source('bronze_data', 'T_BRZ_TIRE_SIZE_BSOTRM') }}
    where ENTRY_TIMESTAMP = (select max_loaded_ts from latest_loaded)
),

-- Step 3: De-duplication (Keep latest record per TIRE_SIZE_CODE)
ranked_data as (
    select *,
        row_number() over (
            partition by TIRE_SIZE_CODE
            order by ENTRY_TIMESTAMP desc, SEQUENCE_NUMBER desc
        ) as rn
    from source_data
),

-- Step 4: Business Logic + Derived Fields
final_data as (
    select
        TIRE_SIZE_CODE,
        TIRE_SIZE_NAME,

        -- Derived: SECTION_WIDTH
        case 
            when upper(trim(TIRE_SIZE_NAME)) = 'SMALL' then 165
            when upper(trim(TIRE_SIZE_NAME)) = 'COMPACT' then 175
            when upper(trim(TIRE_SIZE_NAME)) = 'LARGE' then 215
            when upper(trim(TIRE_SIZE_NAME)) = 'GIANT' then 265
            else null
        end as SECTION_WIDTH,

        -- Derived: ASPECT_RATIO
        case 
            when upper(trim(TIRE_SIZE_NAME)) in ('SMALL', 'COMPACT') then 65
            when upper(trim(TIRE_SIZE_NAME)) = 'LARGE' then 60
            when upper(trim(TIRE_SIZE_NAME)) = 'GIANT' then 55
            else null
        end as ASPECT_RATIO,

        -- Derived: RIM_DIAMETER
        case 
            when upper(trim(TIRE_SIZE_NAME)) = 'SMALL' then 15
            when upper(trim(TIRE_SIZE_NAME)) = 'COMPACT' then 16
            when upper(trim(TIRE_SIZE_NAME)) = 'LARGE' then 17
            when upper(trim(TIRE_SIZE_NAME)) = 'GIANT' then 18
            else null
        end as RIM_DIAMETER,

        -- Derived: TIRE_TYPE
        case 
            when upper(trim(TIRE_SIZE_NAME)) in ('SMALL', 'COMPACT') then 'P'
            when upper(trim(TIRE_SIZE_NAME)) in ('LARGE', 'GIANT') then 'LT'
            else null
        end as TIRE_TYPE,

        -- Derived: METRIC_FLAG
        case 
            when upper(trim(TIRE_SIZE_NAME)) in ('SMALL', 'COMPACT', 'LARGE', 'GIANT') then true
            else null
        end as METRIC_FLAG,

        B9USER,
        B9CYMD,
        B9HMS,
        B9WKSN,

        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        MD5(CONCAT_WS('|', 
            COALESCE(TO_VARCHAR(TIRE_SIZE_CODE), ''),
            COALESCE(TIRE_SIZE_NAME, '')
        )) as RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT

    from ranked_data
    where rn = 1
)

-- Step 5: Final Projection with Explicit Casting
select
    CAST(TIRE_SIZE_CODE AS NUMBER(3,0)) as TIRE_SIZE_CODE,
    CAST(TIRE_SIZE_NAME AS VARCHAR(40)) as TIRE_SIZE_NAME,
    CAST(SECTION_WIDTH AS NUMBER(38,0)) as SECTION_WIDTH,
    CAST(ASPECT_RATIO AS NUMBER(38,0)) as ASPECT_RATIO,
    CAST(RIM_DIAMETER AS NUMBER(38,0)) as RIM_DIAMETER,
    CAST(TIRE_TYPE AS VARCHAR(20)) as TIRE_TYPE,
    CAST(METRIC_FLAG AS BOOLEAN) as METRIC_FLAG,

    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,
    CAST(SOURCE_FILE_NAME AS VARCHAR(200)) as SOURCE_FILE_NAME,
    CAST(BATCH_ID AS VARCHAR(50)) as BATCH_ID,
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,
    CAST(ETL_VERSION AS VARCHAR(20)) as ETL_VERSION,
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,
    CAST(INGESTION_DT AS DATE) as INGESTION_DT
from final_data