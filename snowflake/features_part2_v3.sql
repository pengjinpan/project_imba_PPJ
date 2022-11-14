use database imba;
use warehouse imba_test;

create schema imba1;

//create orders table schema
CREATE or replace TABLE orders(
"order_id" varchar(50),
"user_id" varchar(20),
"eval_set" varchar(20),
"order_number" int,
"order_dow" int,
"order_hour_of_day" int,
"days_since_prior" int NULL,
PRIMARY KEY("order_id")
);

//create order_products_prior table schema
CREATE or replace TABLE order_products_prior(
"order_id" varchar(50),
"product_id" varchar(50),
"add_to_cart_order" int,
"reordered" boolean,
FOREIGN KEY("order_id") REFERENCES
orders("order_id")
);
//FOREIGN KEY("product_id") REFERENCES products("product_id")

//AWS s3 stage for orders 
create or replace stage imba_stage_input
    url = 's3://imba-ppj/data/orders/orders.csv'
    credentials = (aws_secret_key = 'eLO9OkH1g9drP40w3Qjqp5V9JKtuhmIGMBvY+/dO' aws_key_id = 'AKIAXNNKJ53VR3QPC52S');
    
//AWS s3 stage for order_products_prior     
create or replace stage imba_stage_input2
    url = 's3://imba-ppj/data/order_products/order_products__prior.csv.gz'
    credentials = (aws_secret_key = 'eLO9OkH1g9drP40w3Qjqp5V9JKtuhmIGMBvY+/dO' aws_key_id = 'AKIAXNNKJ53VR3QPC52S'); 
    
//copy into snowflake ORDER_PRODUCTS_PRIOR
copy into ORDER_PRODUCTS_PRIOR from @imba_stage_input2
file_format = (
    type = csv
    field_delimiter = ',' skip_header = 1);
    
//copy into snowflake ORDERS
copy into orders from @imba_stage_input
file_format = (type = csv field_delimiter = ',' skip_header = 1);

//top 100 from orders for testing 
-- create or replace table orders_10 like orders;
-- insert into orders_10
-- select TOP 100 * from orders;

//top 100 from ORDER_PRODUCTS_PRIOR for testing 
-- create or replace table ORDER_PRODUCTS_PRIOR_10 like ORDER_PRODUCTS_PRIOR;
-- insert into ORDER_PRODUCTS_PRIOR_10
-- select TOP 100 * from ORDER_PRODUCTS_PRIOR;

//new prior table(orders join order_products_prior)
create or replace table order_products_prior_new  
as (select
od.*,
//od."order_id", 
//od."user_id",
//od."eval_set",
//od."order_number",
//od."order_dow",
//od."order_hour_of_day",
//od."days_since_prior",
//op."order_id",
op."product_id",
op."add_to_cart_order",
op."reordered"

from orders as od
join order_products_prior as op  
on (od."order_id"=op."order_id")
where od."eval_set" = 'prior');


//user_features_1
SELECT "user_id", 
MAX("order_number"), 
sum("days_since_prior"),
avg("days_since_prior")
FROM orders GROUP BY "user_id";

//user_features_2
select "user_id",
count("product_id"),
count(DISTINCT "product_id"),
sum(case when "reordered" = 1 then 1 else 0 end)/sum(case when "order_number" > 1 then 1 else 0 end)
from order_products_prior_new GROUP BY "user_id";

//up_features
select "user_id",
count("order_id"),
min("order_number"),
max("order_number"),
avg("add_to_cart_order")
from order_products_prior_new GROUP BY "user_id","product_id";

//prd_features(product_seq_time)
select "user_id","order_number","product_id",
rank()over(partition by "user_id", "product_id" order by "order_number" asc) as product_seq_time
from order_products_prior_new order by "user_id","order_number";

//using subquery to get sum(product_seq_time = 1&2)
select 
sum(case when product_seq_time = 1 then 1 else 0 end),
sum(case when product_seq_time = 2 then 1 else 0 end)
from
(select "user_id","order_number","product_id",
rank()over(partition by "user_id", "product_id" order by "order_number" asc) as product_seq_time
from order_products_prior_new order by "user_id","order_number");

//using CTE to get sum(product_seq_time = 1&2)
with product_seq_time_table as 
(select "user_id","order_number","product_id",
rank()over(partition by "user_id", "product_id" order by "order_number" asc) as product_seq_time
from order_products_prior_new order by "user_id","order_number")
select 
sum(case when product_seq_time = 1 then 1 else 0 end),
sum(case when product_seq_time = 2 then 1 else 0 end)
from product_seq_time_table





