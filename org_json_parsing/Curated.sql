use database amazon_db;
use schema curated;
create or replace  sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace  sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';



  
7.2 Curated Layer DDL
use schema curated;
-- curated India sales order table
create or replace table amazon_db.curated.in_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);

-- curated US sales order table
create or replace table amazon_db.curated.us_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);
create or replace table amazon_db.curated.final_tbl as(
with  final_in_sales as(
    select t1.*,t2.* from amazon_db.source.in_sales_order t1
left join amazon_db.source.exchange_rate t2
on t1.order_dt = t2.date
where payment_status = 'Paid' and shipping_status = 'Delivered'
),
 ind_sales as(
    select sales_order_key,order_id,order_dt,customer_name,mobile_key,'IND' as country ,'APAC' as Region,
order_quantity, 'INR' as local_currency,unit_price as local_unit_price,promotion_code,final_order_amount as local_final_order_amount,
tax_amount as local_tax_amount,usd2inr as Exchange_rate, final_order_amount/usd2inr as us_total_order_amount,tax_amount/usd2inr as usd_tax_amount,
payment_status,shipping_status,payment_method,payment_provider,mobile as contact_num,shipping_address
from final_in_sales),
final_us_sales as(
    select t1.*,t2.* from amazon_db.source.us_sales_order t1
left join amazon_db.source.exchange_rate_us t2
on t1.order_dt = t2.date
where payment_status = 'Paid' and shipping_status = 'Delivered'
),us_sales as (
select sales_order_key,order_id,order_dt,customer_name,mobile_key,'US' as country ,'NA' as Region,
order_quantity, 'USD' as local_currency,unit_price as local_unit_price,promotion_code,final_order_amount as local_final_order_amount,
tax_amount as local_tax_amount,usd2usd as Exchange_rate, final_order_amount/usd2usd as us_total_order_amount,tax_amount/usd2usd as usd_tax_amount,
payment_status,shipping_status,payment_method,payment_provider,phone as contact_num,shipping_address
from final_us_sales)
select * from ind_sales 
union all
select * from us_sales);
select * from final_tbl;




