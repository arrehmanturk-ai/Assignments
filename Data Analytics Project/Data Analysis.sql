create table df_orders (
		[order_id] int primary key
		,[order_date] date
		,[ship_mode] varchar(20)
		,[segment] varchar(20)
		,[country] varchar(20)
		,[city] varchar(20)
		,[state] varchar(20)
		,[postal_code] varchar(20)
		,[region] varchar(20)
		,[category] varchar(20)
		,[sub_category] varchar(20)
		,[product_id] varchar(50)
		,[quantity] int
		,[discount] decimal(7,2)
		,[sale_price] decimal(7,2)
		,[profit] decimal(7,2)

);

select * from df_orders

-- Top  20 Highest Revenue Generating Products
select Top 20 product_id, sum(sale_price) as sales
from df_orders
group by product_id
order by sales desc;

-- Top 10 Highest Selling Products in each Region
with cte as (
select region,product_id,sum(sale_price) as sales
from df_orders
group by region,product_id)
select * from (
select * 
, ROW_NUMBER() over(partition by region order by sales desc) as rn
from cte) A
where rn <=10;

-- Region Wise Sales and Profit Analysis
select 
    region,
    sum(sale_price) as total_sales,
    sum(profit) as total_profit
from df_orders
group by region
order by total_profit desc;

-- Category Wise and Sub Category Wise Perfomance
select 
    category,
    sub_category,
    sum(sale_price) as total_sales,
    sum(profit) as total_profit
from df_orders
group by category, sub_category
order by total_profit desc;

-- Discount Impact on Profit
select 
    discount,
    count(*) as total_orders,
    sum(sale_price) as total_sales,
    sum(profit) as total_profit
from df_orders
group by discount
order by discount;