use schema consumption;
---v1 
select distinct team_name from(
select first_team as team_name from cricket_db.clean.match_details
union
select second_team as team_name from cricket_db.clean.match_details
);
--v2
insert into team_dim(team_name)
select distinct team_name from(
select first_team as team_name from cricket_db.clean.match_details
union all
select second_team as team_name from cricket_db.clean.match_details
);

----select players with team information.
select a.country,b.team_id,a.player_name
from cricket_db.clean.player_details a
join team_dim b on a.country = b.team_name
group by a.country,b.team_id,a.player_name
having player_name = 'Rashid Khan';


------insert player data into player_dim
insert into player_dim(team_id,player_name)
select b.team_id,a.player_name
from cricket_db.clean.player_details a
join team_dim b on a.country = b.team_name
group by a.country,b.team_id,a.player_name;
select * from player_dim;

---referees dimension
SELECT
  info:officials:match_referees[0]::TEXT AS match_referee,
  info:officials:reserve_umpires[0]::TEXT AS reserve_umpire,
  info:officials:tv_umpires[0]::TEXT AS tv_umpire,
  info:officials:umpires[0]::TEXT AS first_umpire,
  info:officials:umpires[1]::TEXT AS second_umpire
FROM cricket_db.raw.raw_tbl;
---------------------------------

---venue Dimension

insert into venue_dim(venue_name,city)
select venue,city from cricket_db.clean.match_details
group by venue,city;

select * from venue_dim;
------------------

select min(event_date),max(event_date) from cricket_db.clean.match_details;
--
-- Insert hardcoded date values
create or replace table date_range01(date date);
INSERT INTO cricket_db.consumption.date_range01 (Date)
VALUES
  ('2023-10-12'),
  ('2023-10-13'),
  ('2023-10-14'),
  ('2023-10-15'),
  ('2023-10-16'),
  ('2023-10-17'),
  ('2023-10-18'),
  ('2023-10-19'),
  ('2023-10-20'),
  ('2023-10-21'),
  ('2023-10-22'),
  ('2023-10-23'),
  ('2023-10-24'),
  ('2023-10-25'),
  ('2023-10-26'),
  ('2023-10-27'),
  ('2023-10-28'),
  ('2023-10-29'),
  ('2023-10-30'),
  ('2023-10-31'),
  ('2023-11-01'),
  ('2023-11-02'),
  ('2023-11-03'),
  ('2023-11-04'),
  ('2023-11-05'),
  ('2023-11-06'),
  ('2023-11-07'),
  ('2023-11-08'),
  ('2023-11-09'),
  ('2023-11-10');
---------
insert into date_dim
select row_number() over(order by date) as date_id,
date as full_date,
extract(day from date)as day,
extract(month from date)as month,
extract(year from date) as year,
case when extract(quarter from date) in (1,2,3,4) then extract (quarter from date )end as quarter,
dayofweekiso(date)as dayofweek,
EXTRACT(DAY FROM Date) AS DayOfMonth,
    DAYOFYEAR(Date) AS DayOfYear,
    DAYNAME(Date) AS DayOfWeekName,
    CASE When DAYNAME(Date) IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS IsWeekend
from date_range01;
select * from date_dim;

---------------------
INSERT INTO match_type_dim (match_type)
SELECT match_type
FROM cricket_db.clean.match_details
GROUP BY match_type;


insert into match_fact
select 
    m.match_type_number as match_id,
    dd.date_id as date_id,
    0 as referee_id,
    ftd.team_id as first_team_id,
    std.team_id as second_team_id,
    mtd.match_type_id as match_type_id,
    vd.venue_id as venue_id,
    50 as total_overs,
    6 as balls_per_overs,
    max(case when d.country = m.first_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_A,
    sum(case when d.country = m.first_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_A,
    sum(case when d.country = m.first_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_A,
    sum(case when d.country = m.first_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_A,
    0 fours_by_team_a,
    0 sixes_by_team_a,
    (sum(case when d.country = m.first_team then  d.runs else 0 end ) + sum(case when d.country = m.first_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_A,
    sum(case when d.country = m.first_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_a,    
    
    max(case when d.country = m.second_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_B,
    sum(case when d.country = m.second_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_B,
    sum(case when d.country = m.second_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_B,
    sum(case when d.country = m.second_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_B,
    0 fours_by_team_b,
    0 sixes_by_team_b,
    (sum(case when d.country = m.second_team then  d.runs else 0 end ) + sum(case when d.country = m.second_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_B,
    sum(case when d.country = m.second_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_b,
    tw.team_id as toss_winner_team_id,
    m.toss_decision as toss_decision,
    m.matach_result as matach_result,
    mw.team_id as winner_team_id
     
from 
    cricket_db.clean.match_details m
    join date_dim dd on m.event_date = dd.full_dt
    join team_dim ftd on m.first_team = ftd.team_name 
    join team_dim std on m.second_team = std.team_name 
    join match_type_dim mtd on m.match_type = mtd.match_type
    join venue_dim vd on m.venue = vd.venue_name and m.city = vd.city
    join cricket_db.clean.innings_details d  on d.match_type_number = m.match_type_number 
    join team_dim tw on m.toss_winner = tw.team_name 
    join team_dim mw on m.winner= mw.team_name 
    --where m.match_type_number = 4686
    group by
        m.match_type_number,
        date_id,
        referee_id,
        first_team_id,
        second_team_id,
        match_type_id,
        venue_id,
        total_overs,
        toss_winner_team_id,
        toss_decision,
        matach_result,
        winner_team_id;

select * FROM  cricket_db.consumption.match_fact;


insert into delivery_fact
select 
    d.match_type_number as match_id,
    td.team_id,
    bpd.player_id as bower_id, 
    spd.player_id batter_id, 
    nspd.player_id as non_stricker_id,
    d.over,
    d.runs,
    case when d.extra_runs is null then 0 else d.extra_runs end as extra_runs,
    case when d.extra_type is null then 'None' else d.extra_type end as extra_type,
    case when d.player_out is null then 'None' else d.player_out end as player_out,
    case when d.player_out_kind is null then 'None' else d.player_out_kind end as player_out_kind
from 
    cricket_db.clean.innings_details d
    join team_dim td on d.country = td.team_name
    join player_dim bpd on d.bowler = bpd.player_name
    join player_dim spd on d.batter = spd.player_name
    join player_dim nspd on d.non_striker = nspd.player_name;

-- Show delivery Table

select * from delivery_fact;