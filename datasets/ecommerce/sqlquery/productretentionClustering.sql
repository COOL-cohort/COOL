

-- 2. New vs Existing Product retention clustering

with
    transaction_log as (
        select productid,
 				date_trunc('month', reporteddate ) as trunc_month,   
                date_part('month', age(reporteddate, '2021-01-01')) as month_number
                -- instead of 01-01-2021, we can generate min(trans_date) in another CTE above
		from public.ecommerce		
		group by 1, 2, 3),
    time_lapse as (
        select productid, trunc_month, month_number, lag(month_number) over
        (partition by productid order by productid, month_number)
		from transaction_log),
    time_diff as (
        select productid, trunc_month, (month_number - lag) as time_diff
		from time_lapse),

    category as (
        select productid, trunc_month,
			case
				when time_diff = 1 then 'Retained'
				when time_diff > 1 then 'Returning'
				when time_diff is null then 'New'
			end as product_type			
		from time_diff)
select
	trunc_month,	
	product_type,	
	count(productid) as count_products
from category
group by 1, 2
order by 1, 2;