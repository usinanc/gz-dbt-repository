WITH orders_per_day AS (
    SELECT
        date_date,
        COUNT(DISTINCT orders_id) AS nb_transactions,
        ROUND(SUM(CAST(revenue AS FLOAT64)), 0) AS revenue,
        ROUND(SUM(CAST(margin AS FLOAT64)), 0) AS margin,
        ROUND(SUM(CAST(operational_margin AS FLOAT64)), 0) AS operational_margin,
        ROUND(SUM(CAST(purchase_cost AS FLOAT64)), 0) AS purchase_cost,
        ROUND(SUM(CAST(shipping_fee AS FLOAT64)), 0) AS shipping_fee,
        ROUND(SUM(CAST(logcost AS FLOAT64)), 0) AS log_cost,
        ROUND(SUM(CAST(ship_cost AS FLOAT64)), 0) AS ship_cost,
        SUM(CAST(quantity AS FLOAT64)) AS quantity
    FROM {{ ref("int_orders_operational") }}
    GROUP BY date_date
)

SELECT
    date_date,
    revenue,
    margin,
    operational_margin,
    purchase_cost,
    shipping_fee,
    log_cost,
    ship_cost,
    quantity,
    ROUND(revenue / NULLIF(nb_transactions, 0), 2) AS average_basket
FROM orders_per_day
ORDER BY date_date DESC
