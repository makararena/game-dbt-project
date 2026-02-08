{{
    config(
        materialized='table'
    )
}}
/*
  Fact table: one row per game event.
  Enriches events with session_id (matched by player + time window), player attributes,
  and seconds_since_session_start. Events outside any session have session_id = null.
*/

with events as (
    -- Base: one row per event from staging.
    select * from {{ ref('stg_game_events') }}
),

sessions as (
    -- Session time windows for matching events (player_id, session_start_at, session_end_at).
    select
        session_id,
        player_id,
        session_start_at,
        session_end_at,
        platform as session_platform
    from {{ ref('stg_sessions') }}
),

players as (
    -- Player attributes to attach to each event (country, language, difficulty).
    select
        player_id,
        country_code,
        language_code,
        difficulty_selected
    from {{ ref('stg_players') }}
),

events_with_sessions as (
    -- Match events to sessions by player_id and event_at within [session_start_at, session_end_at].
    -- Left join: events with no matching session keep session_id (and session times) null.
    select
        e.event_id,
        e.event_at,
        e.player_id,
        e.event_name,
        e.platform,
        e.game_version,
        e.properties,
        s.session_id,
        s.session_start_at,
        s.session_end_at,
        p.country_code,
        p.language_code,
        p.difficulty_selected
    from events e
    left join sessions s
        on e.player_id = s.player_id
        and e.event_at >= s.session_start_at
        and e.event_at <= s.session_end_at
    left join players p
        on e.player_id = p.player_id
),

final as (
    -- All event columns + session and player context; seconds_since_session_start for in-session events.
    select
        event_id,
        event_at,
        player_id,
        session_id,
        event_name,
        platform,
        game_version,
        properties,
        country_code,
        language_code,
        difficulty_selected,
        session_start_at,
        session_end_at,
        case
            when session_start_at is not null
            then datediff('second', session_start_at, event_at)
            else null
        end as seconds_since_session_start
    from events_with_sessions
)

select * from final
