use database amazon_db;
create schema store_schema;
use schema store_schema;
create or replace table amazon_db.store_schema.in_sales_order (
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 mobile varchar(),
 Delivery_address text
);

create or replace table amazon_db.store_schema.us_sales_order (
raw_file variant
);

create or replace  table amazon_db.store_schema.exchange_rate_us(
    date date, 
    usd2usd decimal(10,7),
    usd2eu decimal(10,7),
    usd2can decimal(10,7),
    usd2uk decimal(10,7),
    usd2inr decimal(10,7),
    usd2jp decimal(10,7)
);

CREATE OR REPLACE TABLE amazon_db.store_schema.mapping_table (
    file_path VARCHAR,
    stage_path string,
    Db_name string,
    schema_name string,
    table_name VARCHAR,
    file_format VARCHAR
    
);
INSERT INTO mapping_table values
    ('raw_stage/csv/', 'amazon_db.store_schema.raw_stage','amazon_db','Store_schema','in_sales_order', '(type = ''CSV'' skip_header = 1 field_delimiter = '','' empty_field_as_null = true)'),
    ('raw_stage/json/', 'amazon_db.store_schema.raw_stage','amazon_db','Store_schema','us_sales_order', '(type = ''json'')'),
    ('raw_stage/parquet/','amazon_db.store_schema.raw_stage','amazon_db','Store_schema','userdata','(type = ''parquet'')');

select * from mapping_table;

create or replace procedure amazon_db.store_schema.copy_table()
returns varchar
language SQL
execute as caller 
as
declare 
data cursor for select * from amazon_db.store_schema.mapping_table;
file_path string;
stage_path string;
db_name string;
schema_name string;
table_name string;
file_format string;
count_files int;
command string;
list string;
result string;

Begin 
result :='';

for rec in data do 
 file_path := rec.file_path;
 stage_path := rec.stage_path;
 db_name := rec.db_name;
 schema_name := rec.schema_name;
 table_name := rec.table_name;
 file_format := rec.file_format;

list @amazon_db.store_schema.raw_stage;

select count(1) into count_files from  table (result_scan(last_query_id()));

if ( :count_files >0 ) then

command := 'copy into ' || :db_name || '.' || :schema_name || '.' || :table_name || '
      from  @'|| : file_path ||'
      file_format = ' || :file_format || '
      on_error = continue
      force = True
      purge = True ';
       execute immediate command;
       result := result || :table_name || ' loaded successfully.\n';
else 
    result := result || :table_name || ' files not avalable .\n';
end if;
end for;
return : result;
end;
call copy_table();







