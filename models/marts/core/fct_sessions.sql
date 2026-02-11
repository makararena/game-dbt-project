{{
    config(
        materialized='table'
    )
}}
/*
  Fact table: one row per gaming session.
  Joins sessions with player attributes and session-level event aggregates
  (total events, deaths, enemies killed, chapters completed, events per minute).
*/

with sessions as (
    -- Base: one row per session from staging.
    select * from {{ ref('stg_sessions') }}
),

players as (
    -- Player attributes to attach to each session (country, language, difficulty).
    select
        player_id,
        country_code,
        language_code,
        difficulty_selected
    from {{ ref('stg_players') }}
),

events_with_sessions as (
    -- Match events to sessions by player_id and event time within session window.
    select
        s.session_id,
        e.event_name,
        e.event_at
    from sessions as s
    inner join {{ ref('stg_game_events') }} as e
        on
            s.player_id = e.player_id
            and s.session_start_at <= e.event_at
            and s.session_end_at >= e.event_at
),

events_agg as (
    -- Per-session event counts and first/last event timestamps.
    select
        session_id,
        count(*) as total_events,
        count(distinct event_name) as unique_event_types,
        count(case when event_name = 'player_died' then 1 end) as deaths_count,
        count(case when event_name = 'enemy_killed' then 1 end) as enemies_killed,
        count(case when event_name = 'chapter_completed' then 1 end) as chapters_completed,
        min(event_at) as first_event_at,
        max(event_at) as last_event_at
    from events_with_sessions
    group by session_id
),

final as (
    -- Sessions + player attributes + event aggregates; events_per_minute = total_events / duration.
    select
        s.session_id,
        s.player_id,
        s.session_start_at,
        s.session_end_at,
        s.platform,
        s.session_duration_minutes,
        p.country_code,
        p.language_code,
        p.difficulty_selected,
        e.unique_event_types,
        e.first_event_at,
        e.last_event_at,
        coalesce(e.total_events, 0) as total_events,
        coalesce(e.deaths_count, 0) as deaths_count,
        coalesce(e.enemies_killed, 0) as enemies_killed,
        coalesce(e.chapters_completed, 0) as chapters_completed,
        case
            when s.session_duration_minutes > 0
                then round(coalesce(e.total_events, 0)::float / s.session_duration_minutes, 2)
            else 0
        end as events_per_minute
    from sessions as s
    left join players as p
        on s.player_id = p.player_id
    left join events_agg as e
        on s.session_id = e.session_id
)

select * from final
