create database org_db;
use org_db;
create schema land;
use schema land;
create or replace stage org_db.land.my_stage;
create or replace file format org_db.land.my_json
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
 from @org_db.land.my_stage/json/ (file_format =>org_db.land.my_json)t;

 select * from projects;
 create schema raw;
create or replace table raw_tbl
(location variant,
organizations variant,
projects variant,
filename text,
modified_date timestamp,
file_row_num int);

create or replace table clean_tbl (
 Country string,
 state string,
 city string,
 name string,
 type string,
 department string,
 project_status string,
 projects string
);



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
create or replace stream raw_stream on table raw_tbl append_only = true  ;
drop stream  raw_stream;
select * from raw_tbl;

CREATE
OR REPLACE TASK parent_task warehouse = 'COMPUTE_WH' SCHEDULE = '2 minute' 
AS copy into org_db.raw.raw_tbl from 
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
insert into org_db.raw.clean_tbl
select t.location:country::text as Country,
 t.location:state::text as state,
 t.location:city::text as city,
 o.value:name::text as name,
 o.value:type::text as type,
 d.value::text as department,
 p.key::text as project_status,
 v.value::text as projects
 from raw_stream t,
 lateral flatten( input=>t.organizations)o,
 lateral flatten( input =>o.value:departments)d,
 lateral flatten( input => t.projects)p,
 lateral flatten (input => p.value)v
 WHERE METADATA$ACTION = 'INSERT'
 and METADATA$ISUPDATE = 'False';
select * from clean_tbl;
create or replace stream clean_stream  on table clean_tbl append_only = true;
create or replace table clean_tbl_history(
 Country string,
 state string,
 city string,
 name string,
 type string,
 department string,
 project_status string,
 projects string
);

create or replace  task data_load_tbl_history
after child_task1
when system$stream_has_data('clean_stream')
as 
insert into clean_tbl_history
select country,state,city,name,type,department,project_status,projects from clean_stream
WHERE METADATA$ACTION = 'INSERT'
 and METADATA$ISUPDATE = 'False';

create or replace task data_load_location
after data_load_tbl_history
as
insert into location(country,state,city,name)
select distinct(country),state,city,name from clean_tbl_history;

create or replace task data_load_orgs
after data_load_location
as 
insert into org_db.raw.orgs (name,type,departments)
select  distinct(name),type,department from clean_tbl_history;

create or replace task data_load_projects
after data_load_orgs
as
insert into org_db.raw.projects(status,project)
select distinct(project_status),projects from clean_tbl_history;

--create a procedure to truncate the clean_tbl_history

CREATE OR REPLACE PROCEDURE truncate_my_table()
RETURNS STRING
LANGUAGE SQL
AS
$$
TRUNCATE org_db.raw.clean_tbl_history;
$$;

create or replace task trunctae_clean_tbl_history
after data_load_projects
as
call truncate_my_table();

alter task trunctae_clean_tbl_history suspend
;
alter task data_load_projects suspend;
alter task data_load_orgs  suspend;
alter task  data_load_location suspend;
alter task data_load_tbl_history suspend;
alter task child_task1  suspend;
alter task parent_task suspend;

list @org_db.land.my_stage/json/;


select * from raw_stream;
select * from clean_stream;
select * from raw_tbl;
select * from clean_tbl;
select * from location;
select * from projects;
select * from orgs;
select * from clean_tbl_history;

---validation

select distinct(departments) from orgs;