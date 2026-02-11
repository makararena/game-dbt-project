{{
    config(
        materialized='table'
    )
}}
/*
  Player dimension: one row per player.
  Enriches player attributes with session aggregates (total sessions, playtime,
  active days, first/last session) and derived fields (days since first seen, days since last session).
*/

with players as (
    -- Base: one row per player from staging.
    select * from {{ ref('stg_players') }}
),

sessions_agg as (
    -- Per-player session aggregates: counts, playtime, first/last session, distinct active days.
    select
        player_id,
        count(*) as total_sessions,
        sum(session_duration_minutes) as total_playtime_minutes,
        avg(session_duration_minutes) as avg_session_duration_minutes,
        min(session_start_at) as first_session_at,
        max(session_start_at) as last_session_at,
        count(distinct date(session_start_at)) as active_days
    from {{ ref('stg_sessions') }}
    group by player_id
),

final as (
    -- Players + session aggregates; 0 where no sessions; days_since_* from current_timestamp.
    select
        p.player_id,
        p.first_seen_at,
        p.country_code,
        p.language_code,
        p.difficulty_selected,
        s.avg_session_duration_minutes,
        s.first_session_at,
        s.last_session_at,
        coalesce(s.total_sessions, 0) as total_sessions,
        coalesce(s.total_playtime_minutes, 0) as total_playtime_minutes,
        coalesce(s.active_days, 0) as active_days,
        datediff('day', p.first_seen_at, current_timestamp()) as days_since_first_seen,
        case
            when s.last_session_at is not null
                then datediff('day', s.last_session_at, current_timestamp())
        end as days_since_last_session
    from players as p
    left join sessions_agg as s
        on p.player_id = s.player_id
)

select * from final
