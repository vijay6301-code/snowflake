use schema clean;
create or replace table cricket_db.clean.match_details
as
select 
info:match_type_number::int as match_type_number,
info:event.name::text as event_name,
info:dates[0]::date as event_date,
info:match_type::text as match_type,
info:season::text as season,
info:team_type::text as team_type,
info:overs::int as overs,
info:city::text as city,
info:venue::text as venue,
info:gender::text as gender,
info:teams[0]::text as first_team,
info:teams[1]::text as second_team,
case 
        when info:outcome.winner is not null then 'Result Declared'
        when info:outcome.result = 'tie' then 'Tie'
        when info:outcome.result = 'no result' then 'No Result'
        else info:outcome.result
    end as matach_result,
    case 
        when info:outcome.winner is not null then info:outcome.winner
        else 'NA'
    end as winner,
     info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision,
    --
    file_name ,
    file_row_number,
    file_hash_key,
  stg_modified_time 
    from 
    cricket_db.raw.RAW_TBL;

select * from cricket_db.clean.match_details
order by match_type_number;

create or replace table player_details as
select 
info:match_type_number::int as match_type_number,
p.key::text as country,
t.value::text as player_name,
file_name ,
    file_row_number,
    file_hash_key,
  stg_modified_time 
  from cricket_db.raw.raw_tbl,
  lateral flatten(input => info:players)p,
  lateral flatten(input =>p.value)t;

  select * from player_details



;
select * from cricket_db.raw.raw_tbl limit 1;

select 
p.key::text as team,
t.value::text as player_name  from cricket_db.raw.raw_tbl,
lateral flatten(input => info:players)p,
lateral flatten(p.value)t
limit 22;

alter table cricket_db.clean.match_details
add constraint pk_match_type_number primary key (match_type_number);
desc table player_details; 

alter table player_details
add constraint fk_match_id
foreign key(match_type_number)
references cricket_db.clean.match_details(match_type_number);


create or replace table innings_details as
select 
    m.info:match_type_number::int as match_type_number, 
    i.value:team::text as country,
    o.value:over::int+1 as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::number as extra_runs,
    w.value:player_out::text as player_out,
    w.value:kind::text as player_out_kind,
    w.value:fielders::variant as player_out_fielders,
   m.file_name ,
    m.file_row_number,
    m.file_hash_key,
  m.stg_modified_time 
from cricket_db.raw.raw_tbl m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten (input => o.value:deliveries) d,
lateral flatten (input => d.value:extras, outer => True) e,
lateral flatten (input => d.value:wickets, outer => True) w;

ALTER TABLE cricket_db.clean.innings_details
MODIFY COLUMN match_type_number SET NOT NULL;

ALTER TABLE cricket_db.clean.innings_details
MODIFY COLUMN country SET NOT NULL;

ALTER TABLE cricket_db.clean.innings_details
MODIFY COLUMN over SET NOT NULL;

ALTER TABLE cricket_db.clean.innings_details
MODIFY COLUMN bowler SET NOT NULL;

ALTER TABLE cricket_db.clean.innings_details
MODIFY COLUMN batter SET NOT NULL;

ALTER TABLE cricket_db.clean.innings_details
MODIFY COLUMN non_striker SET NOT NULL;

alter table cricket_db.clean.innings_details
add constraint fk_innings_match_id
foreign key (match_type_number)
references cricket_db.clean.match_details(match_type_number);

select * from innings_details;



