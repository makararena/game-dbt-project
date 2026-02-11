{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('raw', 'raw_game_events') }}

),

renamed as (

    select
        event_id,
        player_id,
        game_version,
        properties,
        try_to_timestamp(event_time) as event_at,
        lower(event_name) as event_name,
        lower(platform) as platform
    from source

)

select * from renamed
