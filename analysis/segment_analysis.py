import sqlite3
import pandas as pd

DB_PATH = "ecommerce_funnel.db"

conn = sqlite3.connect(DB_PATH)

query = """
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
    'traffic_source' AS segment_type,
    traffic_source AS segment_value,
    SUM(viewed_product) AS product_views,
    SUM(added_to_cart) AS add_to_carts,
    SUM(started_checkout) AS checkout_starts,
    SUM(purchased) AS purchases,
    ROUND(100.0 * SUM(added_to_cart) / SUM(viewed_product), 2) AS view_to_cart_rate,
    ROUND(100.0 * SUM(started_checkout) / SUM(added_to_cart), 2) AS cart_to_checkout_rate,
    ROUND(100.0 * SUM(purchased) / SUM(started_checkout), 2) AS checkout_to_purchase_rate,
    ROUND(100.0 * SUM(purchased) / SUM(viewed_product), 2) AS overall_purchase_rate
FROM funnel_flags
GROUP BY traffic_source

UNION ALL

SELECT
    'region' AS segment_type,
    region AS segment_value,
    SUM(viewed_product) AS product_views,
    SUM(added_to_cart) AS add_to_carts,
    SUM(started_checkout) AS checkout_starts,
    SUM(purchased) AS purchases,
    ROUND(100.0 * SUM(added_to_cart) / SUM(viewed_product), 2) AS view_to_cart_rate,
    ROUND(100.0 * SUM(started_checkout) / SUM(added_to_cart), 2) AS cart_to_checkout_rate,
    ROUND(100.0 * SUM(purchased) / SUM(started_checkout), 2) AS checkout_to_purchase_rate,
    ROUND(100.0 * SUM(purchased) / SUM(viewed_product), 2) AS overall_purchase_rate
FROM funnel_flags
GROUP BY region;
"""

df = pd.read_sql_query(query, conn)
conn.close()

for segment in ["device_type", "traffic_source", "region"]:
    segment_df = df[df["segment_type"] == segment].copy()
    segment_df = segment_df.sort_values("overall_purchase_rate", ascending=False)

    print()
    print(f"Segment Performance: {segment}")
    print("-" * 60)
    print(
        segment_df[
            [
                "segment_value",
                "view_to_cart_rate",
                "cart_to_checkout_rate",
                "checkout_to_purchase_rate",
                "overall_purchase_rate",
            ]
        ].to_string(index=False)
    )

    best = segment_df.iloc[0]
    worst = segment_df.iloc[-1]

    print()
    print(
        f"Best {segment}: {best['segment_value']} "
        f"({best['overall_purchase_rate']:.2f}% overall purchase rate)"
    )
    print(
        f"Worst {segment}: {worst['segment_value']} "
        f"({worst['overall_purchase_rate']:.2f}% overall purchase rate)"
    )
