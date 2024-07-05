create database org_db;
use org_db;
create schema land;
use schema land;
create or replace stage my_stage;
create or replace file format my_json
type =json
null_if = ('\\n','null','')
strip_outer_array = true;

--check if the file is landed
list @my_stage/json;

--check if the data is coming corectly
select t.$1:location::object as location,
t.$1:organizations::variant as organizations,
t.$1:projects::variant as projects,
----
metadata$filename,
metadata$file_last_modified,
metadata$file_row_number
 from @my_stage/json/org.gz (file_format =>'my_json')t;

 select * from projects;
 create schema raw;
create or replace table raw_tbl
(location variant,
organizations variant,
projects variant,
filename text,
modified_date timestamp,
file_row_num int);


create or replace table location (
    loc_id int primary key autoincrement,
    country text not null,
    state text not null,
    city text not null,
    name text not null
);

create or replace table orgs(
    org_id int primary key autoincrement,
    name text,
    type text,
    departments text
)
;
create table projects(
    proj_id int autoincrement,
    status text,
    project text
);
create or replace stream raw_stream on table raw_tbl append_only = true;

CREATE
OR REPLACE TASK parent_task warehouse = 'COMPUTE_WH' SCHEDULE = '2 minute' 
AS copy into raw_tbl from 
(select t.$1:location::object as location,
t.$1:organizations::variant as organizations,
t.$1:projects::variant as projects,
----
metadata$filename,
metadata$file_last_modified,
metadata$file_row_number
 from @org_db.land.my_stage/json/ (file_format =>org_db.land.my_json)t
)
 on_error=continue
 purge = true;

create or replace task child_task1  
after 
parent_task
when system$stream_has_data('raw_stream') as
insert into clean_tbl
select t.location:country::text as Country,
 t.location:state::text as state,
 t.location:city::text as city,
 o.value:name::text as name,
 o.value:type::text as type,
 d.value::text as department,
 p.key::text as project_status,
 v.value::text as projects
 from raw_tbl t,
 lateral flatten( input=>t.organizations)o,
 lateral flatten( input =>o.value:departments)d,
 lateral flatten( input => t.projects)p,
 lateral flatten (input => p.value)v
;

create or replace task child_task2
after  child_task1
as 
insert into location (country,state,city,name ) 
select distinct(country),state,city,name from clean_tbl;

create or replace task child_task3
after child_task2
as 
insert into orgs (name,type,departments)
select  distinct(name),type,department from clean_tbl;

create or replace task child_task4
after child_task3
as
insert into projects(status,project)
select distinct(project_status),projects from clean_tbl;


alter task child_task4 suspend;
alter task child_task3 suspend;
alter task child_task2 suspend;
alter task child_task1 suspend;
alter task parent_task suspend;

truncate table raw_tbl;
truncate table clean_tbl;
select * from location;
select * from  projects;
select * from orgs;