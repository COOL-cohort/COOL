/* This table creates the first seen date marker and calculates the monthy asin count - # of months after first purchase */
WITH t_first_seen AS (
  SELECT date,
    DATE_DIFF(date, first_seen_date, MONTH) AS month_order,
    FORMAT_DATETIME('%Y%m', date) AS first_seen,
    Product_ID
  FROM (
      SELECT DATE(TIMESTAMP(Reported_Date)) AS date,
        Product_ID,
        FIRST_VALUE(DATE(TIMESTAMP(Reported_Date))) OVER (
          PARTITION BY Product_ID
          ORDER BY DATE(TIMESTAMP(Reported_Date))
        ) AS first_seen_date
      FROM merchantsalemthly
    )
),
/* This table computes the aggregate asin count per first seen cohort and month order */
t_agg AS (
  SELECT first_seen,
    month_order,
    COUNT(DISTINCT Product_ID) AS ASINs
  FROM t_first_seen
  GROUP BY first_seen,
    month_order
),
/* This table computes the repeat rate of ASINs */
t_cohort AS (
  SELECT *,
    SAFE_DIVIDE(ASINs, CohortASINs) AS CohortASINsPerc
  FROM (
      SELECT *,
        FIRST_VALUE(ASINs) OVER (
          PARTITION BY first_seen
          ORDER BY month_order
        ) AS CohortASINs
      FROM t_agg
    )
)
SELECT *
FROM t_cohort
ORDER BY first_seen,
  month_order