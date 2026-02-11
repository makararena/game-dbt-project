{{
    config(
        materialized='table'
    )
}}
/*
  Cohort retention: one row per (cohort_date, country_code, difficulty_selected, days_since_cohort).
  Cohorts are defined by player first_seen_at date. Outputs active_players, cohort_size,
  and retention_rate_pct (share of cohort active on that day).
*/

with players as (
    -- Cohort definition: player_id, cohort_date = date(first_seen_at), country, difficulty.
    select
        player_id,
        country_code,
        difficulty_selected,
        date(first_seen_at) as cohort_date
    from {{ ref('stg_players') }}
),

sessions as (
    -- One row per (player, session date) for activity.
    select
        player_id,
        date(session_start_at) as session_date
    from {{ ref('stg_sessions') }}
),

player_sessions as (
    -- Each (player, cohort, session_date) with session_date >= cohort_date; days_since_cohort computed.
    select distinct
        p.player_id,
        p.cohort_date,
        p.country_code,
        p.difficulty_selected,
        s.session_date,
        datediff('day', p.cohort_date, s.session_date) as days_since_cohort
    from players as p
    inner join sessions as s
        on
            p.player_id = s.player_id
            and p.cohort_date <= s.session_date
),

cohort_retention as (
    -- Per cohort and days_since_cohort: active_players count; cohort_size = cohort size (same for all days in cohort).
    select
        cohort_date,
        country_code,
        difficulty_selected,
        days_since_cohort,
        count(distinct player_id) as active_players,
        first_value(count(distinct player_id)) over (
            partition by cohort_date, country_code, difficulty_selected
            order by days_since_cohort
            rows between unbounded preceding and unbounded following
        ) as cohort_size
    from player_sessions
    group by
        cohort_date,
        country_code,
        difficulty_selected,
        days_since_cohort
),

final as (
    -- Add retention_rate_pct = (active_players / cohort_size) * 100.
    select
        cohort_date,
        country_code,
        difficulty_selected,
        days_since_cohort,
        active_players,
        cohort_size,
        round(
            (active_players::float / nullif(cohort_size, 0)) * 100,
            2
        ) as retention_rate_pct
    from cohort_retention
    where retention_rate_pct < 100
)

select * from final
order by cohort_date desc, days_since_cohort asc, country_code asc, difficulty_selected asc
