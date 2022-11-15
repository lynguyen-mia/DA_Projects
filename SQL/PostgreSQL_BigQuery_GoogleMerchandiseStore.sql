-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT 
    LEFT(date, 6) AS month, 
    SUM(totals.visits) AS visits, 
    SUM(totals.pageviews) AS pageviews, 
    SUM(totals.transactions) AS transactions, 
    SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _table_suffix BETWEEN '20170101' AND '20170331'
GROUP BY month
ORDER BY month;


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT 
    trafficSource.source AS source, 
    SUM(totals.visits) AS total_visits, 
    SUM(totals.bounces) AS total_no_of_bounces, 
    SUM(totals.bounces)*100.0/SUM(totals.visits) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;


-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
SELECT -- month data table
    "Month" AS time_type,
    LEFT(date,6) AS time,
    trafficSource.source AS source, 
    SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY time, source
UNION ALL
SELECT -- union all week data table
    "Week" AS time_type,
    FORMAT_DATE("%Y%W",PARSE_DATE("%Y%m%d",date)) AS time,
    trafficSource.source AS source, 
    SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY time, source
ORDER BY source, time;


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
-- SOLUTION 1
WITH purchase AS -- purchaser type table
    (SELECT
        LEFT(date,6) AS month,
        SUM(totals.pageviews) AS purchase_pageviews,
        COUNT(DISTINCT fullVisitorId) AS purchase_user
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _table_suffix BETWEEN '20170601' AND '20170731'
        AND totals.transactions	>= 1
    GROUP BY month),
non_purchase AS -- non-purchaser type table
    (SELECT
        LEFT(date,6) AS month,
        SUM(totals.pageviews) AS non_purchase_pageviews,
        COUNT(DISTINCT fullVisitorId) AS non_purchase_user
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _table_suffix BETWEEN '20170601' AND '20170731'
        AND totals.transactions	IS NULL
    GROUP BY month)
SELECT -- Average number of pageviews by purchaser type
    p.month, 
    p.purchase_pageviews/p.purchase_user AS avg_pageviews_purchase,
    n.non_purchase_pageviews/n.non_purchase_user AS avg_pageviews_non_purchase
FROM purchase AS p
INNER JOIN non_purchase AS n
    ON p.month = n.month
ORDER BY month;

-- SOLUTION 2
#standardSQL
WITH cte AS
    (SELECT
        LEFT(date,6) AS month,
        SUM(CASE WHEN totals.transactions >= 1 THEN totals.pageviews ELSE NULL END) AS purchase_pageviews,
        SUM(CASE WHEN totals.transactions IS NULL THEN totals.pageviews ELSE NULL END) AS non_purchase_pageviews,
        COUNT(DISTINCT CASE WHEN totals.transactions >= 1 THEN fullVisitorId ELSE NULL END) AS purchase_user,
        COUNT(DISTINCT CASE WHEN totals.transactions IS NULL THEN fullVisitorId ELSE NULL END) AS non_purchase_user
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _table_suffix BETWEEN '20170601' AND '20170731'
    GROUP BY month)
SELECT
    month,
    purchase_pageviews/purchase_user AS avg_pageviews_purchase,
    non_purchase_pageviews/non_purchase_user AS avg_pageviews_non_purchase
FROM cte
ORDER BY month;


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT
    LEFT(date,6) AS month,
    (SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId)) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions	>= 1  
GROUP BY month;


-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
    LEFT(date,6) AS month,
    ROUND(AVG(totals.totalTransactionRevenue),2) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL 
GROUP BY month;


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
WITH customer AS -- find customers who purchased "YouTube Men's Vintage Henley"
    (SELECT 
        fullVisitorId
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
        UNNEST(hits) AS hits,
        UNNEST(hits.product) AS product
        WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
            AND product.productRevenue IS NOT NULL)
SELECT -- Other products purchased by customers who purchased product "YouTube Men's Vintage Henley"
    product.v2ProductName AS other_purchased_products,
    SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) AS hits,
UNNEST(hits.product) AS product
WHERE fullVisitorId IN (SELECT fullVisitorId FROM customer)
    AND product.v2ProductName <> "YouTube Men's Vintage Henley"
    AND product.productRevenue IS NOT NULL
GROUP BY other_purchased_products
ORDER BY quantity DESC;


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH data_table AS -- count product views, add-to-card, purchase numbers
    (SELECT
        LEFT(date,6) AS month,
        COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN 1 ELSE null END) AS num_product_view,
        COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN 1 ELSE null END) AS num_addtocart,
        COUNT(CASE WHEN eCommerceAction.action_type = '6' THEN 1 ELSE null END) AS num_purchase
    FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
    WHERE _table_suffix BETWEEN '20170101' AND '20170331'
    GROUP BY month)
SELECT *, -- calculate add-to-card rate, purchase rate
    ROUND((num_addtocart*100.0/num_product_view),2) AS add_to_cart_rate,
    ROUND((num_purchase*100.0/num_product_view),2) AS purchase_rate
FROM data_table
ORDER BY month;