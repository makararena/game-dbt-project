{{
    config(
        materialized='table'
    )
}}
/*
  Daily active players: one row per (session_date, platform, country_code, difficulty_selected).
  Aggregates active_players (distinct count), total_sessions, total_playtime_minutes,
  and averages per player (avg_sessions_per_player, avg_playtime_minutes_per_player).
*/

with sessions as (
    -- Per (player, date, platform): session count and total playtime.
    select
        player_id,
        platform,
        date(session_start_at) as session_date,
        count(distinct session_id) as sessions_count,
        sum(session_duration_minutes) as total_playtime_minutes
    from {{ ref('stg_sessions') }}
    group by
        player_id,
        date(session_start_at),
        platform
),

players as (
    -- Player attributes for slicing (country, difficulty).
    select
        player_id,
        country_code,
        difficulty_selected
    from {{ ref('stg_players') }}
),

final as (
    -- Roll up by date, platform, country, difficulty: active_players, totals, and per-player averages.
    select
        s.session_date,
        s.platform,
        p.country_code,
        p.difficulty_selected,
        count(distinct s.player_id) as active_players,
        sum(s.sessions_count) as total_sessions,
        sum(s.total_playtime_minutes) as total_playtime_minutes,
        round(avg(s.sessions_count), 2) as avg_sessions_per_player,
        round(avg(s.total_playtime_minutes), 2) as avg_playtime_minutes_per_player
    from sessions as s
    left join players as p
        on s.player_id = p.player_id
    group by
        s.session_date,
        s.platform,
        p.country_code,
        p.difficulty_selected
)

select * from final
order by session_date desc, platform asc, country_code asc, difficulty_selected asc
