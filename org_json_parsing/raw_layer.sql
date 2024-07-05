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
 on_error=continue
 purge = true;

 select * from raw_tbl;
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