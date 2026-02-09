-- tests/sessions_no_overlap.sql
-- Fails if a player has overlapping sessions

with sessions as (

    select
        session_id,
        player_id,
        session_start,
        session_end
    from {{ source('raw', 'raw_sessions') }}

),

overlaps as (

    select
        s1.player_id,
        s1.session_id as session_id_1,
        s2.session_id as session_id_2,
        s1.session_start as s1_start,
        s1.session_end   as s1_end,
        s2.session_start as s2_start,
        s2.session_end   as s2_end
    from sessions s1
    join sessions s2
      on s1.player_id = s2.player_id
     and s1.session_id < s2.session_id -- чтобы не сравнивать строку с самой собой
     and s1.session_start < s2.session_end
     and s2.session_start < s1.session_end
)

select *
from overlaps
