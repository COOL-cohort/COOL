

/*Sample description:
    1. This table could be in many contexts: selling products,
    2. reporteddate from Jan 2021 to Dec 2021.
The Manager wants to know more about Product Retenion which is one of the key metrics that influences revenue. */


-- 1. Product Retention over time
with start_month as (
    select productid
		from public.ecommerce 		
		group by productid
		having to_char(min(reporteddate), 'yyyy-mm') = '2021-01') -- we can adjust difference month to indicate difference result		
select 
	to_char(reporteddate, 'yyyy-mm') as transaction_ym,	
	count(distinct productid) as count_product	
from public.ecommerce 
where productid in
      (select productid from start_month)
group by 1;