create database my_db;

create or replace file format schema_evol_fmt
type = 'csv'
compression = 'auto'
field_delimiter = ','
parse_header = True
record_delimiter = '\n'
ERROR_ON_COLUMN_COUNT_MISMATCH = false;

create or replace stage my_stage
file_format =schema_evol_fmt;

CREATE OR REPLACE TABLE emp
USING TEMPLATE (
SELECT ARRAY_AGG(object_construct(*))
FROM TABLE(
INFER_SCHEMA(
LOCATION=>'@my_stage',
FILE_FORMAT=>'schema_evol_fmt'
)))
enable_schema_evolution = true;

copy into emp
from @my_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

select * from emp;

