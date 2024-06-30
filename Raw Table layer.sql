use schema raw;

create or replace table cricket_db.raw.Raw_tbl(
meta object not null,
info variant not null,
innings variant not null,
file_name text not null,
file_row_number int not null,
file_hash_key text not null,
stg_modified_time timestamp not null
);
copy into cricket_db.raw.raw_tbl from(
select $1:meta::object as meta,
$1:info::variant as info,
$1:innings::array as innings,
metadata$filename,
metadata$file_row_number int,
metadata$file_content_key text,
metadata$file_last_modified stg_modified_ts
from @cricket_db.land.my_stage/cricket/json
(file_format => cricket_db.land.my_json)
)
on_error = continue;
select * from cricket_db.raw.raw_tbl;