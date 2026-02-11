{{
    config(
        materialized='table'
    )
}}
/*
  Session funnel: one row per (session_date, platform, country_code, difficulty_selected).
  Counts sessions that reached each funnel step (game_started, chapter_started, checkpoint_reached,
  chapter_completed, game_closed), conversion rates (pct of total_sessions), and avg counts/duration.
*/

with sessions as (
    -- Base: one row per session (for matching events by time window).
    select
        session_id,
        player_id,
        session_start_at,
        session_end_at,
        platform,
        session_duration_minutes
    from {{ ref('stg_sessions') }}
),

events as (
    -- Events to match to sessions (player_id, event_name, event_at).
    select
        player_id,
        event_name,
        event_at
    from {{ ref('stg_game_events') }}
),

session_events as (
    -- Per session: flags for key funnel events (has_*) and counts (e.g. chapters_started_count).
    select
        s.session_id,
        s.player_id,
        s.session_start_at,
        s.session_end_at,
        s.platform,
        s.session_duration_minutes,
        max(case when e.event_name = 'game_started' then 1 else 0 end) as has_game_started,
        max(case when e.event_name = 'chapter_started' then 1 else 0 end) as has_chapter_started,
        max(case when e.event_name = 'checkpoint_reached' then 1 else 0 end) as has_checkpoint_reached,
        max(case when e.event_name = 'chapter_completed' then 1 else 0 end) as has_chapter_completed,
        max(case when e.event_name = 'game_closed' then 1 else 0 end) as has_game_closed,
        count(case when e.event_name = 'game_started' then 1 end) as game_started_count,
        count(case when e.event_name = 'chapter_started' then 1 end) as chapters_started_count,
        count(case when e.event_name = 'checkpoint_reached' then 1 end) as checkpoints_reached_count,
        count(case when e.event_name = 'chapter_completed' then 1 end) as chapters_completed_count
    from sessions as s
    left join events as e
        on
            s.player_id = e.player_id
            and s.session_start_at <= e.event_at
            and s.session_end_at >= e.event_at
    group by
        s.session_id,
        s.player_id,
        s.session_start_at,
        s.session_end_at,
        s.platform,
        s.session_duration_minutes
),

players as (
    -- Player attributes for grouping (country, difficulty).
    select
        player_id,
        country_code,
        difficulty_selected
    from {{ ref('stg_players') }}
),

final as (
    -- Roll up by date, platform, country, difficulty: funnel counts, conversion pct, and averages.
    select
        se.platform,
        p.country_code,
        p.difficulty_selected,
        date(se.session_start_at) as session_date,
        count(distinct se.session_id) as total_sessions,
        sum(se.has_game_started) as sessions_with_game_started,
        sum(se.has_chapter_started) as sessions_with_chapter_started,
        sum(se.has_checkpoint_reached) as sessions_with_checkpoint_reached,
        sum(se.has_chapter_completed) as sessions_with_chapter_completed,
        sum(se.has_game_closed) as sessions_with_game_closed,
        round(
            (sum(se.has_game_started)::float / nullif(count(distinct se.session_id), 0)) * 100,
            2
        ) as game_started_rate_pct,
        round(
            (sum(se.has_chapter_started)::float / nullif(count(distinct se.session_id), 0)) * 100,
            2
        ) as chapter_started_rate_pct,
        round(
            (sum(se.has_checkpoint_reached)::float / nullif(count(distinct se.session_id), 0)) * 100,
            2
        ) as checkpoint_reached_rate_pct,
        round(
            (sum(se.has_chapter_completed)::float / nullif(count(distinct se.session_id), 0)) * 100,
            2
        ) as chapter_completed_rate_pct,
        round(avg(se.chapters_started_count), 2) as avg_chapters_started,
        round(avg(se.checkpoints_reached_count), 2) as avg_checkpoints_reached,
        round(avg(se.chapters_completed_count), 2) as avg_chapters_completed,
        round(avg(se.session_duration_minutes), 2) as avg_session_duration_minutes
    from session_events as se
    left join players as p
        on se.player_id = p.player_id
    group by
        date(se.session_start_at),
        se.platform,
        p.country_code,
        p.difficulty_selected
)

select * from final
order by session_date desc, platform asc, country_code asc, difficulty_selected asc
