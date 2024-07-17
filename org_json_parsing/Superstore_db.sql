create database superstore;
use superstore;
create or replace schema raw_schema;
use schema raw_schema;

create or replace file format csv_format
type = csv
field_delimiter = ','
parse_header = true
null_if = ('null','null')
empty_field_as_null = true
field_optionally_enclosed_by = '\042'
DATE_FORMAT = 'DD-MM-YYYY'
ENCODING = 'UTF8'
REPLACE_INVALID_CHARACTERS = TRUE --set this parameter if file contain UTF charecters
ERROR_ON_COLUMN_COUNT_MISMATCH = false
compression = auto;

create or replace stage my_stage
file_format ='csv_format';

---To check the columns 
SELECT *FROM TABLE(
                INFER_SCHEMA (
                    LOCATION=>'@my_stage/Superstore.csv.gz'
                    , FILE_FORMAT=>'csv_format'
                    , IGNORE_CASE=>TRUE
                ));
---to create table from the inferschema
CREATE OR REPLACE TABLE superstore
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA (
                    LOCATION=>'@my_stage/'
                    , FILE_FORMAT=>'csv_format'
                    , IGNORE_CASE=>TRUE
                )
            )
    );
      copy into superstore
      from @my_stage
      on_error =continue 
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

list @my_stage;

select * from superstore;
UPDATE superstore
SET ship_DATE= REPLACE(ORDER_DATE, '/', '-');
truncate superstore;
    
CREATE OR REPLACE TABLE Dim_Products (
    PRODUCT_ID text PRIMARY KEY,
    CATEGORY STRING,
    SUB_CATEGORY STRING,
    PRODUCT_NAME STRING
);

CREATE OR REPLACE TABLE Dim_Customers (
    CUSTOMER_ID text PRIMARY KEY,
    CUSTOMER_NAME STRING,
    SEGMENT STRING,
    COUNTRY STRING,
    CITY STRING,
    STATE STRING,
    POSTAL_CODE STRING,
    REGION STRING
);

CREATE OR REPLACE TABLE Dim_Orders (
    ORDER_ID text PRIMARY KEY,
    ORDER_DATE DATE,
    SHIP_MODE STRING
);

CREATE OR REPLACE TABLE FactSales (
    ROW_ID INT PRIMARY KEY,
    ORDER_ID text,
    CUSTOMER_ID text,
    PRODUCT_ID text,
    SHIP_DATE DATE,
    SALES DECIMAL,
    QUANTITY INT,
    DISCOUNT DECIMAL,
    PROFIT DECIMAL,
    FOREIGN KEY (ORDER_ID) REFERENCES Dim_Orders(ORDER_ID),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES Dim_Customers(CUSTOMER_ID),
    FOREIGN KEY (PRODUCT_ID) REFERENCES Dim_Products(PRODUCT_ID)
);

insert into dim_products
select distinct(product_id),category,sub_category,product_name  from superstore ;

insert into dim_customers
select customer_id,customer_name,segment,country,city,state,postal_code,Region from(
select customer_id,customer_name,segment,country,city,state,postal_code,Region,
row_number() over(partition by customer_id order by customer_id)as rn from superstore
qualify rn = 1);
insert into dim_orders 
select distinct(order_id),to_date(order_date,'MM-DD-YYYY')as order_date,ship_mode from superstore;
select * from dim_orders;

insert into factsales
select row_id,order_id,customer_id,product_id,
to_date(ship_date,'MM-DD-YYYY')as ship_date,
sales,quantity,discount,profit from superstore;
select * from factsales;
----------------------top 3 customers who made highesst sales
select d.customer_name,sum(f.sales) total from dim_customers d
join  factsales f
on f.customer_id = d.customer_id
group by d.customer_name
order by sum(f.sales) desc
limit 3;


---------get all customers sales category and sub category wise
select d.customer_name,p.category,p.sub_category,sum(f.sales)as total from dim_customers D
join factsales f on
f.customer_id = d.customer_id
join dim_products p on
p.product_id = f.product_id
group by all;


----latest year month on month sales percentage analysis
with cte as (
select year(o.order_date)as year,month(o.order_date)as month_number,to_char(o.order_date,'mon')as month,sum(f.sales)as total from dim_orders o 
join factsales f on
o.order_id = f.order_id
where year =(select(max(year(o.order_date))) from dim_orders o)
group by all
order by month(o.order_date) asc
),
cte2 as 
(select month,total,lag(total,1,total) over(order by month_number)as prev_month_sales from cte)
select *,concat(round((total-prev_month_sales)/prev_month_sales * 100,2),'%') as status
from cte2;






