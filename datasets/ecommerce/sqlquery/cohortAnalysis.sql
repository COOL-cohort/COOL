


-- 3. Cohort Analysis

with
    min_date as (
        select productid, min(reporteddate) as min_reporteddate
		from public.ecommerce	
		group by productid),

    cohort_month as (
        select productid, date_trunc('month', min_reporteddate) as cohort_month
		from min_date),		

    prod_log as (
        select a.productid, date_part('month', age(date_trunc('month', a.reporteddate), b.cohort_month)) as month_number
		from public.ecommerce a 
		left join cohort_month b		
		on a.productid = b.productid
		group by 1, 2),		

    month_number_size as (
        select b.cohort_month, a.month_number, count(*) as count_user
		from prod_log a
		left join cohort_month b		
		on a.productid = b.productid  
		group by 1, 2),			

    cohort_size as (
        select cohort_month, count(*) as count_user
		from cohort_month
		group by 1)

select
	m.cohort_month,	
	c.count_user,	
	m.month_number,	
	(m.count_user::float/c.count_user) as retention_rate	
from month_number_size m
left join cohort_size c
on m.cohort_month = c.cohort_month
order by 1, 3;