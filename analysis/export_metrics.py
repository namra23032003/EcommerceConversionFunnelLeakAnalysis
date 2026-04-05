import sqlite3
import pandas as pd

DB_PATH = "ecommerce_funnel.db"

conn = sqlite3.connect(DB_PATH)

# Overall funnel metrics
overall_query = """
WITH funnel_flags AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS viewed_product,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
        MAX(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS started_checkout,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_user_events
    GROUP BY user_id
),
funnel_counts AS (
    SELECT
        SUM(viewed_product) AS product_views,
        SUM(added_to_cart) AS add_to_carts,
        SUM(started_checkout) AS checkout_starts,
        SUM(purchased) AS purchases
    FROM funnel_flags
)
SELECT
    product_views,
    add_to_carts,
    checkout_starts,
    purchases,
    ROUND(100.0 * add_to_carts / product_views, 2) AS view_to_cart_rate,
    ROUND(100.0 * checkout_starts / add_to_carts, 2) AS cart_to_checkout_rate,
    ROUND(100.0 * purchases / checkout_starts, 2) AS checkout_to_purchase_rate,
    ROUND(100.0 * purchases / product_views, 2) AS overall_purchase_rate
FROM funnel_counts;
"""

# Segment funnel metrics
segment_query = """
WITH funnel_flags AS (
    SELECT
        user_id,
        device_type,
        traffic_source,
        region,
        MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS viewed_product,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
        MAX(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS started_checkout,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_user_events
    GROUP BY user_id, device_type, traffic_source, region
)

SELECT
    'device_type' AS segment_type,
    device_type AS segment_value,
    SUM(viewed_product) AS product_views,
    SUM(added_to_cart) AS add_to_carts,
    SUM(started_checkout) AS checkout_starts,
    SUM(purchased) AS purchases,
    ROUND(100.0 * SUM(added_to_cart) / SUM(viewed_product), 2) AS view_to_cart_rate,
    ROUND(100.0 * SUM(started_checkout) / SUM(added_to_cart), 2) AS cart_to_checkout_rate,
    ROUND(100.0 * SUM(purchased) / SUM(started_checkout), 2) AS checkout_to_purchase_rate,
    ROUND(100.0 * SUM(purchased) / SUM(viewed_product), 2) AS overall_purchase_rate
FROM funnel_flags
GROUP BY device_type

UNION ALL

SELECT
    'traffic_source',
    traffic_source,
    SUM(viewed_product),
    SUM(added_to_cart),
    SUM(started_checkout),
    SUM(purchased),
    ROUND(100.0 * SUM(added_to_cart) / SUM(viewed_product), 2),
    ROUND(100.0 * SUM(started_checkout) / SUM(added_to_cart), 2),
    ROUND(100.0 * SUM(purchased) / SUM(started_checkout), 2),
    ROUND(100.0 * SUM(purchased) / SUM(viewed_product), 2)
FROM funnel_flags
GROUP BY traffic_source

UNION ALL

SELECT
    'region',
    region,
    SUM(viewed_product),
    SUM(added_to_cart),
    SUM(started_checkout),
    SUM(purchased),
    ROUND(100.0 * SUM(added_to_cart) / SUM(viewed_product), 2),
    ROUND(100.0 * SUM(started_checkout) / SUM(added_to_cart), 2),
    ROUND(100.0 * SUM(purchased) / SUM(started_checkout), 2),
    ROUND(100.0 * SUM(purchased) / SUM(viewed_product), 2)
FROM funnel_flags
GROUP BY region;
"""

overall_df = pd.read_sql_query(overall_query, conn)
segment_df = pd.read_sql_query(segment_query, conn)

overall_df.to_csv("data/funnel_summary_metrics.csv", index=False)
segment_df.to_csv("data/segment_funnel_metrics.csv", index=False)

conn.close()

print("Metrics exported successfully.")
print("Files created:")
print("data/funnel_summary_metrics.csv")
print("data/segment_funnel_metrics.csv")
