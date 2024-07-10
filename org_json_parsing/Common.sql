use database amazon_db;
alter  warehouse my_warehouse
set
warehouse_size = 'small'
min_cluster_count = 1
max_cluster_count = 1;
create schema if not exists source; -- will have source stage etc
create schema if not exists curated; -- data curation and de-duplication
create schema if not exists consumption; -- fact & dimension
create or replace schema  common; -- for file formats sequence object etc

create or replace stage amazon_db.common.my_stage;
create or replace file format amazon_db.common.csv_format
type = csv
field_delimiter = ','
skip_header = 1
null_if = ('null','null')
empty_field_as_null = true
field_optionally_enclosed_by = '\042'
compression = auto;

create or replace file format amazon_db.common.json_format
type = json
strip_outer_array = true
  compression = auto;



