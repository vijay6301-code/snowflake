use schema consumption;
create or replace sequence region_dim
 start = 1 increment = 1 order;

create or replace  table region_dim(
  region_id_pk number primary key,
    Country text, 
    Region text,
    isActive text(1)
);

CREATE  or replace      SEQUENCE product_dim_seq
    START =1
    INCREMENT = 1
    order
   ;
create or replace  drop table product_dim(
    product_id_pk number primary key,
    mobile_key text,
    Brand text, 
    Model text,
    Color text,
    RAM text,
    Storage text,
    isActive text(1)
);


create or replace sequence customer_dim_seq start = 1 increment = 1 order;
create or replace transient table customer_dim(
    customer_id_pk number primary key,
    customer_name text,
    CONCTACT_NO text,
    SHIPPING_ADDRESS text,
    country text,
    region text,
    isActive text(1)
);

create or  replace sequence payment_dim_seq start = 1 increment = 1 order;
create or replace transient table payment_dim(
    payment_id_pk number primary key,
    PAYMENT_METHOD text,
    PAYMENT_PROVIDER text,
    country text,
    region text,
    isActive text(1)
);


create or replace table sales_fact (
 order_code varchar(),
 region_id_fk number(38,0),
 customer_id_fk number(38,0),
 payment_id_fk number(38,0),
 product_id_fk number(38,0),
 order_quantity number(38,0),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number,
 us_total_order_amt number,
 usd_tax_amt number
);



alter table sales_fact add
    constraint fk_sales_region FOREIGN KEY (REGION_ID_FK) REFERENCES region_dim (REGION_ID_PK) NOT ENFORCED;



alter table sales_fact add
    constraint fk_sales_customer FOREIGN KEY (CUSTOMER_ID_FK) REFERENCES customer_dim (CUSTOMER_ID_PK) NOT ENFORCED;
--
alter table sales_fact add
    constraint fk_sales_payment FOREIGN KEY (PAYMENT_ID_FK) REFERENCES payment_dim (PAYMENT_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_product FOREIGN KEY (PRODUCT_ID_FK) REFERENCES product_dim (PRODUCT_ID_PK) NOT ENFORCED;

  
insert into region_dim
select region_dim_seq.nextval as region_id_pk,country,region,'Y' as IsActive from (
    select country,region,count(*) from final_tbl
group by country,region);

select * from region_dim;




insert into product_dim select product_dim_seq.nextval as product_id_pk,mobile_key,Brand,Model,color,RAM,Storage,'Y' as IsActive from(
select mobile_key,t[0]::text as Brand,
t[1]::text as model,
t[2]::text as color,
t[3]::text as RAM,
t[4]::text as Storage, row_number() over(partition by  mobile_key order by  mobile_key)as rn 
from(
select mobile_key,split(mobile_key,'/')as t from final_tbl);
select mobile_key from (
select mobile_key,t[0]::text as Brand,
t[1]::text as model,
t[2]::text as color,
t[3]::text as RAM,
t[4]::text as Storage,row_number() over (partition by mobile_key order by mobile_key)as rn
from(
select mobile_key,split(mobile_key,'/')as t from final_tbl))
where rn = 1;



insert into customer_dim
select customer_dim_seq.nextval as customer_id_pk,customer_name,contact_num,shipping_address,country,region,'Y' as IsActive from final_tbl;



insert into payment_dim 
select payment_dim_seq.nextval as payment_id_pk,payment_method,payment_provider,country,region,'Y' IsActive from(
select payment_method,payment_provider,country,Region,count(*) from final_tbl
group by all
);
select * from payment_dim;

select f.* from  final_tbl f
inner join customer_dim c on f.country = c.country and f.customer_name = c.customer_name and f.region = c.region
inner join region_dim r on f.country = r.country and f.region = c.region
inner join product_dim on 
