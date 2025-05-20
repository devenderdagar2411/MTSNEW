{{ config(
    materialized = 'table',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_TIRE_SIZE'
) }}

with source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        B9BSCD,         -- TIRE_SIZE_CODE (Key field)
        B9NAME,         -- TIRE_SIZE_NAME (Data field)
        B9USER,
        B9CYMD,
        B9HMS,
        B9WKSN,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT
    from RAW_DATA.BRONZE_SALES.T_BRZ_TIRE_SIZE_BSOTRM
),

ranked_data as (
    select *,
        row_number() over (
            partition by B9BSCD
            order by ENTRY_TIMESTAMP desc, SEQUENCE_NUMBER desc
        ) as rn
    from source_data
),

final_data as (
    select
        B9BSCD as TIRE_SIZE_CODE,
        B9NAME as TIRE_SIZE_NAME,

        -- Derived: SECTION_WIDTH based on B9NAME
        case 
            when trim(upper(B9NAME)) = 'SMALL' then 165
            when trim(upper(B9NAME)) = 'COMPACT' then 175
            when trim(upper(B9NAME)) = 'LARGE' then 215
            when trim(upper(B9NAME)) = 'GIANT' then 265
            else null
        end as SECTION_WIDTH,

        -- Derived: ASPECT_RATIO - fixed or inferred (customize as needed)
        case 
            when trim(upper(B9NAME)) in ('SMALL', 'COMPACT') then 65
            when trim(upper(B9NAME)) = 'LARGE' then 60
            when trim(upper(B9NAME)) = 'GIANT' then 55
            else null
        end as ASPECT_RATIO,

        -- Derived: RIM_DIAMETER - fixed or inferred (customize as needed)
        case 
            when trim(upper(B9NAME)) = 'SMALL' then 15
            when trim(upper(B9NAME)) = 'COMPACT' then 16
            when trim(upper(B9NAME)) = 'LARGE' then 17
            when trim(upper(B9NAME)) = 'GIANT' then 18
            else null
        end as RIM_DIAMETER,

        -- Derived: TIRE_TYPE - example logic
        case 
            when trim(upper(B9NAME)) in ('SMALL', 'COMPACT') then 'P'
            when trim(upper(B9NAME)) in ('LARGE', 'GIANT') then 'LT'
            else null
        end as TIRE_TYPE,

        -- Derived: METRIC_FLAG - example logic
        case 
            when trim(upper(B9NAME)) in ('SMALL', 'COMPACT', 'LARGE', 'GIANT') then true
            else null
        end as METRIC_FLAG,

        -- B9USER as LAST_MAINTAINED_USER,
        -- B9CYMD as LAST_MAINTAINED_DATE,
        -- B9HMS as LAST_MAINTAINED_TIME,
        -- B9WKSN as LAST_MAINTAINED_WORKSTATION,

        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        MD5(TO_VARCHAR(B9BSCD) || '|' || COALESCE(B9NAME, '')) as RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT,

        -- Surrogate key
        ABS(HASH(B9BSCD)) as TIRE_SIZE_KEY

    from ranked_data
    where rn = 1
)

select
    CAST(TIRE_SIZE_KEY AS BIGINT) as TIRE_SIZE_KEY,
    CAST(TIRE_SIZE_CODE AS INTEGER) as TIRE_SIZE_CODE,
    CAST(TIRE_SIZE_NAME AS VARCHAR(40)) as TIRE_SIZE_NAME,
    CAST(SECTION_WIDTH AS INTEGER) as SECTION_WIDTH,
    CAST(ASPECT_RATIO AS INTEGER) as ASPECT_RATIO,
    CAST(RIM_DIAMETER AS INTEGER) as RIM_DIAMETER,
    CAST(TIRE_TYPE AS VARCHAR(20)) as TIRE_TYPE,
    CAST(METRIC_FLAG AS BOOLEAN) as METRIC_FLAG,
    -- CAST(LAST_MAINTAINED_USER AS VARCHAR(10)) as LAST_MAINTAINED_USER,
    -- CAST(LAST_MAINTAINED_DATE AS INTEGER) as LAST_MAINTAINED_DATE,
    -- CAST(LAST_MAINTAINED_TIME AS INTEGER) as LAST_MAINTAINED_TIME,
    -- CAST(LAST_MAINTAINED_WORKSTATION AS VARCHAR(10)) as LAST_MAINTAINED_WORKSTATION,
    CAST(SOURCE_SYSTEM AS VARCHAR(100)) as SOURCE_SYSTEM,
    CAST(SOURCE_FILE_NAME AS VARCHAR(255)) as SOURCE_FILE_NAME,
    CAST(BATCH_ID AS VARCHAR(100)) as BATCH_ID,
    CAST(RECORD_CHECKSUM_HASH AS VARCHAR(64)) as RECORD_CHECKSUM_HASH,
    CAST(ETL_VERSION AS VARCHAR(50)) as ETL_VERSION,
    CAST(INGESTION_DTTM AS TIMESTAMP_NTZ) as INGESTION_DTTM,
    CAST(INGESTION_DT AS DATE) as INGESTION_DT
from final_data
