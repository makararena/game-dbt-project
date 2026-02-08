{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('raw', 'raw_players') }}

),

renamed as (

    select
        player_id,
        try_to_timestamp(first_seen_at) as first_seen_at,
        upper(country) as country_code,
        lower(language) as language_code,
        lower(difficulty_selected) as difficulty_selected
    from source

)

select * from renamed
