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
        try_to_timestamp(event_time) as event_at,
        player_id,
        lower(event_name) as event_name,
        lower(platform) as platform,
        game_version,
        properties
    from source

)

select * from renamed
