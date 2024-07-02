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