use database amazon_db;
create schema store_schema;

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
 shipping_address varchar()
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
create or replace stage my_stage;
create or replace file format amazon_db.store_schema.csv_format
type = csv
field_delimiter = ','
skip_header = 1
null_if = ('null','null')
empty_field_as_null = true
field_optionally_enclosed_by = '\042'
compression = auto;

create or replace file format amazon_db.store_schema.json_format
type = json
strip_outer_array = true
  compression = auto;


create or replace table load_details
(
stage_table_name string,
schema_name string,
database_name string,
file_loc string,
fyles_type string,
feild_delim string,
on_error string,
skip_header int,
force boolean,
is_active boolean,
primary key (stage_table_name,schema_name,database_name)
);
insert into load_details values(
    'in_sales_order','Store_schema','Amazon_db','my_stage','CSV',',','continue',1,true, 
    true
),
('us_sales_order','Store_schema','Amazon_db','my_stage','json',null,'continue',null,true,true);

select * from load_details;

create or replace procedure amazon_db.store_schema.sp_automate_data_copy()
returns varchar
language sql
execute as caller 
as
declare 
curs  cursor for select * from amazon_db.store_schema.load_details where is_active = true;

tbl string;
sch string ;
db string ;
file_type string;
fld_dm string;
st_loc string;
skp_hdr int;
forc string;
on_err string;
ret string;
file_format string;
cnt int;
copy_stmt string;
create_stage_stmt  string;

Begin
ret :='';
for rec in curs
do 
tbl := rec.stage_table_name;
sch := rec.schema_name;
db := rec.database_name;
file_type := rec.fyles_type;
fld_dm :=rec.feild_delim;
st_loc :=rec.file_loc;
skp_hdr := rec.skip_header;
forc := rec.force;
on_err := rec.on_error;

if (:file_type = 'CSV') then
    file_format := '(type = ''' || :file_type || ''' skip_header = ' || :skp_hdr || ' field_delimiter = ''' || :fld_dm || ''' empty_field_as_null = true)';
else 
    file_format := '(type = ''' || :file_type || ''')';
end if;

list @amazon_db.store_schema.raw_stage;

select count(1) into cnt from  table (result_scan(last_query_id()));

if (: cnt >0 ) then
    copy_stmt := 'copy into ' || :db || '.' || :sch || '.' || :tbl || '
      from  @amazon_db.store_schema.raw_stage
      file_format = ' || :file_format || '
      on_error = continue
      force = true
      purge = true';

      execute immediate copy_stmt;
      ret := ret || :file_type || ' format files completed successfully.\n';
else
    ret := ret || :file_type || ' format files not available.\n';
end if;
end for;
return : ret;
end;


call amazon_db.store_schema.sp_automate_data_copy();