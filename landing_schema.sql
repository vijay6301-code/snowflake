
create database cricket_db;
create or replace schema land;
use schema land;
create or replace schema Raw;
create or replace schema clean;
create or replace schema consumption;
create or replace file format my_json
type =json
null_if = ('\\n','null','')
strip_outer_array = true;
create or replace stage my_stage;

show stages;
list @my_stage/cricket/json;
select 
        t.$1:meta::object as meta, 
        t.$1:info::variant as info, 
        t.$1:innings::array as innings, 
        --
        metadata$filename,
        metadata$file_row_number,
        metadata$file_content_key,
        metadata$file_last_modified
    from @cricket_db.land.my_stage/cricket/json (file_format => 'cricket.land.my_json') t
 



 

