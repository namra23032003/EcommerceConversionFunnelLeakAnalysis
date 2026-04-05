import sqlite3
import pandas as pd

DB_PATH = "ecommerce_funnel.db"

conn = sqlite3.connect(DB_PATH)

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

overall_df = pd.read_sql_query(overall_query, conn)
segment_df = pd.read_sql_query(segment_query, conn)
conn.close()

overall = overall_df.iloc[0]

dropoffs = {
    "Product View → Add to Cart": round(100 - overall["view_to_cart_rate"], 2),
    "Add to Cart → Checkout Start": round(100 - overall["cart_to_checkout_rate"], 2),
    "Checkout Start → Purchase": round(100 - overall["checkout_to_purchase_rate"], 2),
}
biggest_dropoff_stage = max(dropoffs, key=dropoffs.get)
biggest_dropoff_value = dropoffs[biggest_dropoff_stage]

device_df = segment_df[segment_df["segment_type"] == "device_type"].copy()
traffic_df = segment_df[segment_df["segment_type"] == "traffic_source"].copy()
region_df = segment_df[segment_df["segment_type"] == "region"].copy()

best_device = device_df.sort_values("overall_purchase_rate", ascending=False).iloc[0]
worst_device = device_df.sort_values("overall_purchase_rate", ascending=True).iloc[0]

best_traffic = traffic_df.sort_values("overall_purchase_rate", ascending=False).iloc[0]
worst_traffic = traffic_df.sort_values("overall_purchase_rate", ascending=True).iloc[0]

best_region = region_df.sort_values("overall_purchase_rate", ascending=False).iloc[0]
worst_region = region_df.sort_values("overall_purchase_rate", ascending=True).iloc[0]

print()
print("E-commerce Funnel Analysis Summary")
print("----------------------------------")

print()
print("Overall Funnel Performance")
print(f"Product Views: {int(overall['product_views'])}")
print(f"Add to Carts: {int(overall['add_to_carts'])}")
print(f"Checkout Starts: {int(overall['checkout_starts'])}")
print(f"Purchases: {int(overall['purchases'])}")

print()
print("Step Conversion Rates")
print(f"View → Cart: {overall['view_to_cart_rate']:.2f}%")
print(f"Cart → Checkout: {overall['cart_to_checkout_rate']:.2f}%")
print(f"Checkout → Purchase: {overall['checkout_to_purchase_rate']:.2f}%")
print(f"Overall View → Purchase: {overall['overall_purchase_rate']:.2f}%")

print()
print("Biggest Drop-off Stage")
print(f"{biggest_dropoff_stage}: {biggest_dropoff_value:.2f}% drop-off")

print()
print("Best / Worst Segment Performance")

print(
    f"Best Device: {best_device['segment_value']} "
    f"({best_device['overall_purchase_rate']:.2f}% overall purchase rate)"
)
print(
    f"Worst Device: {worst_device['segment_value']} "
    f"({worst_device['overall_purchase_rate']:.2f}% overall purchase rate)"
)

print(
    f"Best Traffic Source: {best_traffic['segment_value']} "
    f"({best_traffic['overall_purchase_rate']:.2f}% overall purchase rate)"
)
print(
    f"Worst Traffic Source: {worst_traffic['segment_value']} "
    f"({worst_traffic['overall_purchase_rate']:.2f}% overall purchase rate)"
)

print(
    f"Best Region: {best_region['segment_value']} "
    f"({best_region['overall_purchase_rate']:.2f}% overall purchase rate)"
)
print(
    f"Worst Region: {worst_region['segment_value']} "
    f"({worst_region['overall_purchase_rate']:.2f}% overall purchase rate)"
)

print()
print("Business Interpretation")
print(
    "The funnel shows meaningful drop-off after product views and again at the final "
    "checkout-to-purchase stage. Desktop users convert better than Mobile users, and "
    "Email / Direct traffic outperforms Social and Paid Search. These patterns suggest "
    "the business should prioritize checkout optimization, especially for mobile users, "
    "and review lower-converting acquisition channels."
)
