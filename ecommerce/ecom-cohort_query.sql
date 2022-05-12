/* This table creates the first seen date marker and calculates the monthy asin count - # of months after first purchase */
WITH t_first_seen AS (
  SELECT 
  date,
  DATE_DIFF(date, first_seen_date, MONTH) AS month_order,
  FORMAT_DATETIME('%Y%m', date) AS first_seen,
  asin_id
  FROM (
    SELECT 
    DATE(TIMESTAMP(rpt_dt)) AS date,
    asin_id,
    FIRST_VALUE(DATE(TIMESTAMP(rpt_dt))) OVER (PARTITION BY asin_id ORDER BY DATE(TIMESTAMP(rpt_dt))) AS first_seen_date
    FROM wmt-mtech-assortment-ml-prod.us_1010_dl_secure.ecomm_merchant_sales_mthly 
    )
  ),

/* This table computes the aggregate asin count per first seen cohort and month order */
t_agg AS (
  SELECT 
  first_seen,
  month_order,
  COUNT(DISTINCT asin_id) AS ASINs
  FROM 
  t_first_seen
  GROUP BY first_seen, month_order
),

/* This table computes the repeat rate of ASINs */
 t_cohort AS (
  SELECT *,
  SAFE_DIVIDE(ASINs, CohortASINs) AS CohortASINsPerc
  FROM (
      SELECT *,
      FIRST_VALUE(ASINs) OVER (PARTITION BY first_seen ORDER BY month_order) AS CohortASINs
      FROM t_agg
  )
 )

SELECT * FROM t_cohort 
ORDER BY first_seen, month_order