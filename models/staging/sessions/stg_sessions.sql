{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('raw', 'raw_sessions') }}

),

renamed as (

    select
        session_id,
        player_id,
        try_to_timestamp(session_start) as session_start_at,
        try_to_timestamp(session_end) as session_end_at,
        lower(platform) as platform,
        datediff('minute', try_to_timestamp(session_start), try_to_timestamp(session_end)) as session_duration_minutes
    from source

)

select * from renamed
