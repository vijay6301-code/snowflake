create schema raw;
create or replace table raw_tbl
(location variant,
organizations variant,
projects variant,
filename text,
modified_date timestamp,
file_row_num int);

copy into raw_tbl from 
(select t.$1:location::object as location,
t.$1:organizations::variant as organizations,
t.$1:projects::variant as projects,
----
metadata$filename,
metadata$file_last_modified,
metadata$file_row_number
 from @org_db.land.my_stage/json/ (file_format =>org_db.land.my_json)t
)
 on_error=continue;

 select * from raw_tbl;
create or replace table clean_tbl as 
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
insert into location (country,state,city,name ) 
select distinct(country),state,city,name from clean_tbl;

select * from location;

insert into orgs (name,type,departments)
select  distinct(name),type,department from clean_tbl;

insert into projects(status,project)
select distinct(project_status),projects from clean_tbl;
;
select  * from projects;

select * from raw_tbl;
select * from clean_tbl;

truncate table raw_tbl;