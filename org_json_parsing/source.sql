use schema source;
create or replace sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';
  show sequences;

  -- India Sales Table in Source Schema (CSV File)
create or replace table amazon_db.source.in_sales_order (
 sales_order_key number(38,0),
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
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- US Sales Table in Source Schema (Parquet File)
create or replace table amazon_db.source.us_sales_order (
 sales_order_key number(38,0),
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
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

create or replace  table amazon_db.source.exchange_rate_us(
    date date, 
    usd2usd decimal(10,7),
    usd2eu decimal(10,7),
    usd2can decimal(10,7),
    usd2uk decimal(10,7),
    usd2inr decimal(10,7),
    usd2jp decimal(10,7)
);


---sql script to ingest the data into source from stage
copy into in_sales_order from (
            select   in_sales_order_seq.nextval, 
            t.$1::text as order_id,
            t.$2::text as customer_name,
            t.$3::text as mobile_key,
            t.$4::number as order_quantity,
            t.$5::number as unit_price, 
            t.$6::number as order_valaue, 
            t.$7::text as promotion_code , 
            t.$8::number(10,2)  as final_order_amount,
            t.$9::number(10,2) as tax_amount,
            t.$10::date as order_dt,
            t.$11::text as payment_status,
            t.$12::text as shipping_status,
            t.$13::text as payment_method,
            t.$14::text as payment_provider,
            t.$15::text as mobile,
            t.$16::text as shipping_address,
            metadata$filename as stg_file_name,
            metadata$file_row_number as stg_row_numer,
            metadata$file_last_modified as stg_last_modified from @amazon_db.common.my_stage/csv/ (file_format => amazon_db.common.csv_format ) t
)
on_error = continue 
purge = true;

---sql script to ingest the data into source from stage

copy into us_sales_order from(                                
                select                              
                us_sales_order_seq.nextval, 
                $1:"Order ID"::text as order_id,   
                $1:"Customer Name"::text as customer_name,
                $1:"Mobile Model"::text as mobile_key,
                to_number($1:"Quantity") as quantity,
                to_number($1:"Price per Unit") as unit_price,
                to_decimal($1:"Total Price") as total_price,
                $1:"Promotion Code"::text as promotion_code,
                $1:"Order Amount"::number(10,2) as order_amount,
                to_decimal($1:"Tax") as tax,
                $1:"Order Date"::date as order_dt,
                $1:"Payment Status"::text as payment_status,
                $1:"Shipping Status"::text as shipping_status,
                $1:"Payment Method"::text as payment_method,
                $1:"Payment Provider"::text as payment_provider,
                $1:"Phone"::text as phone,
                $1:"Delivery Address"::text as shipping_address,
                metadata$filename as stg_file_name,
                metadata$file_row_number as stg_row_numer,
                metadata$file_last_modified as stg_last_modified
 from @amazon_db.common.my_stage/json/ (file_format => amazon_db.common.json_format ) t)
 on_error =continue
 purge =true
;

copy into amazon_db.source. exchange_rate_us from (
    select to_date(t.$1,'DD-MM-YYYY')as date,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7 from
 @amazon_db.common.my_stage/csv/ 
(file_format => amazon_db.common.csv_format) t
)
on_error = continue
purge = true;



select * from amazon_db.source.in_sales_order;
select * from  amazon_db.source.exchange_rate;