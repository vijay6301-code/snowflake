create or replace database inter_db;
use inter_db;
----1.find user id who is missed 3 consecutive days
create or replace  table session(
    userId int,sessionDate date,Iscompleted int
);
insert into session values(1,to_date('2022-02-01','YYYY-MM-DD'),0),(1,to_date('2022-02-02','YYYY-MM-DD'),0),
(2,to_date('2022-02-02','YYYY-MM-DD'),1),(2,to_date('2022-02-03','YYYY-MM-DD'),0),(2,to_date('2022-02-04','YYYY-MM-DD'),0),
(3,to_date('2022-02-02','YYYY-MM-DD'),1),(4,to_date('2022-02-03','YYYY-MM-DD'),0),(4,to_date('2022-02-04','YYYY-MM-DD'),0);
select * from session;
with CTE as (
    select *,row_number() over (partition by userid,iscompleted order by sessiondate) as rn from session 
),cte2 as (select *,rn -iscompleted as missingcons from cte)

select distinct(userid) from cte2
where iscompleted = 0 and missingcons =3;

--2 find the person who made highest number of sales in each quarter
create or replace table sales(
    id int,sales_date date
);
insert into sales values (1,to_date('02-04-2024','MM-DD-YYYY')),(2,to_date('03-04-2024','MM-DD-YYYY')),(3,to_date('03-06-2024','MM-DD-YYYY')),
(1,to_date('04-04-2024','MM-DD-YYYY')),(1,to_date('08-04-2024','MM-DD-YYYY')),(2,to_date('04-04-2024','MM-DD-YYYY')),(2,to_date('06-04-2024','MM-DD-YYYY')),
(1,to_date('09-04-2024','MM-DD-YYYY')),(2,to_date('10-04-2024','MM-DD-YYYY')),(3,to_date('05-04-2024','MM-DD-YYYY')),(3,to_date('10-04-2024','MM-DD-YYYY')),
(3,to_date('12-04-2024','MM-DD-YYYY'));
select * from sales;
insert into sales values (1,to_date('06-04-2024','MM-DD-YYYY'));

with CTE as (
    select *,quarter(sales_date)as qtr from sales
),cte2 as
(select id,qtr,count(*)as numberofsales from cte
group by id,qtr),cte3 as (
    select *,dense_rank() over (partition by qtr order by numberofsales desc)as rn from cte2
)
select qtr,id,numberofsales  from cte3
where rn =1;

--3 find the most significant increse in visits month over month
create or replace table web(
    pageid int,visitdate date,visits int);
insert into web values(1,to_date('01-04-2024','MM-DD-YYYY'),50),(1,to_date('01-04-2024','MM-DD-YYYY'),20),(1,to_date('02-04-2024','MM-DD-YYYY'),100),
(2,to_date('01-04-2024','MM-DD-YYYY'),40),(1,to_date('01-05-2024','MM-DD-YYYY'),50),(2,to_date('02-04-2024','MM-DD-YYYY'),100);
insert into web values (1,to_date('02-04-2024','MM-DD-YYYY'),50);
select * from web;
with cte as (
    select pageid,month(visitdate)as month,sum(visits)as visits from web
    group by pageid,month(visitdate)),
cte2 as (select * ,lag(visits,1,visits) over (partition by pageID order by month)as previous_month from cte),
cte3 as (select *,visits -previous_month as inc from cte2),
cte4 as (select pageid,dense_rank() over (order by inc desc)as rn from cte3)
select pageid from cte4
where rn = 1;
