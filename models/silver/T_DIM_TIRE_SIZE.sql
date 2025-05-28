{{ config(
    materialized = 'table',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_TIRE_SIZE'
) }}

-- Step 1: Source Extraction
with source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        B9BSCD,  -- TIRE_SIZE_CODE (Key field)
        B9NAME,  -- TIRE_SIZE_NAME (Data field)
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
    from  {{ source('bronze_data', 'T_BRZ_TIRE_SIZE_BSOTRM') }}
),

-- Step 2: De-duplication (Keep latest record per TIRE_SIZE_CODE)
ranked_data as (
    select *,
        row_number() over (
            partition by B9BSCD
            order by ENTRY_TIMESTAMP desc, SEQUENCE_NUMBER desc
        ) as rn
    from source_data
),

-- Step 3: Business Logic + Derived Fields
final_data as (
    select
        B9BSCD as TIRE_SIZE_CODE,
        B9NAME as TIRE_SIZE_NAME,

        -- Derived: SECTION_WIDTH
        case 
            when upper(trim(B9NAME)) = 'SMALL' then 165
            when upper(trim(B9NAME)) = 'COMPACT' then 175
            when upper(trim(B9NAME)) = 'LARGE' then 215
            when upper(trim(B9NAME)) = 'GIANT' then 265
            else null
        end as SECTION_WIDTH,

        -- Derived: ASPECT_RATIO
        case 
            when upper(trim(B9NAME)) in ('SMALL', 'COMPACT') then 65
            when upper(trim(B9NAME)) = 'LARGE' then 60
            when upper(trim(B9NAME)) = 'GIANT' then 55
            else null
        end as ASPECT_RATIO,

        -- Derived: RIM_DIAMETER
        case 
            when upper(trim(B9NAME)) = 'SMALL' then 15
            when upper(trim(B9NAME)) = 'COMPACT' then 16
            when upper(trim(B9NAME)) = 'LARGE' then 17
            when upper(trim(B9NAME)) = 'GIANT' then 18
            else null
        end as RIM_DIAMETER,

        -- Derived: TIRE_TYPE
        case 
            when upper(trim(B9NAME)) in ('SMALL', 'COMPACT') then 'P'
            when upper(trim(B9NAME)) in ('LARGE', 'GIANT') then 'LT'
            else null
        end as TIRE_TYPE,

        -- Derived: METRIC_FLAG
        case 
            when upper(trim(B9NAME)) in ('SMALL', 'COMPACT', 'LARGE', 'GIANT') then true
            else null
        end as METRIC_FLAG,

        B9USER,
        B9CYMD,
        B9HMS,
        B9WKSN,

        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        MD5(TO_VARCHAR(B9BSCD) || '|' || COALESCE(B9NAME, '')) as RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT,

        -- Surrogate Key
        ABS(HASH(B9BSCD)) as TIRE_SIZE_KEY

    from ranked_data
    where rn = 1
)

-- Step 4: Final Projection with Explicit Casting
select
    cast(TIRE_SIZE_KEY as bigint) as TIRE_SIZE_KEY,
    cast(TIRE_SIZE_CODE as integer) as TIRE_SIZE_CODE,
    cast(TIRE_SIZE_NAME as varchar(40)) as TIRE_SIZE_NAME,
    cast(SECTION_WIDTH as integer) as SECTION_WIDTH,
    cast(ASPECT_RATIO as integer) as ASPECT_RATIO,
    cast(RIM_DIAMETER as integer) as RIM_DIAMETER,
    cast(TIRE_TYPE as varchar(20)) as TIRE_TYPE,
    cast(METRIC_FLAG as boolean) as METRIC_FLAG,

    -- Optional audit fields (commented out if unused)
    cast(B9USER as varchar(10)) as LAST_MAINTAINED_USER,
    cast(B9CYMD as integer) as LAST_MAINTAINED_DATE,
    cast(B9HMS as integer) as LAST_MAINTAINED_TIME,
    cast(B9WKSN as varchar(10)) as LAST_MAINTAINED_WORKSTATION,

    cast(SOURCE_SYSTEM as varchar(100)) as SOURCE_SYSTEM,
    cast(SOURCE_FILE_NAME as varchar(255)) as SOURCE_FILE_NAME,
    cast(BATCH_ID as varchar(100)) as BATCH_ID,
    cast(RECORD_CHECKSUM_HASH as varchar(64)) as RECORD_CHECKSUM_HASH,
    cast(ETL_VERSION as varchar(50)) as ETL_VERSION,
    cast(INGESTION_DTTM as timestamp_ntz) as INGESTION_DTTM,
    cast(INGESTION_DT as date) as INGESTION_DT
from final_data
