import random
from datetime import datetime, timedelta
import pandas as pd

random.seed(42)

NUM_USERS = 5000
START_DATE = datetime(2025, 1, 1)

devices = ["Mobile", "Desktop", "Tablet"]
traffic_sources = ["Organic Search", "Paid Search", "Direct", "Social", "Email", "Referral"]
regions = ["North", "South", "East", "West", "Central"]
product_categories = ["Electronics", "Fashion", "Home", "Beauty", "Sports"]

device_weights = [0.5, 0.35, 0.15]
traffic_weights = [0.25, 0.20, 0.20, 0.15, 0.10, 0.10]
region_weights = [0.20, 0.20, 0.20, 0.20, 0.20]
category_weights = [0.25, 0.25, 0.20, 0.15, 0.15]

records = []

for user_num in range(1, NUM_USERS + 1):
    user_id = f"U{user_num:05d}"
    session_id = f"S{user_num:05d}"

    device_type = random.choices(devices, weights=device_weights, k=1)[0]
    traffic_source = random.choices(traffic_sources, weights=traffic_weights, k=1)[0]
    region = random.choices(regions, weights=region_weights, k=1)[0]
    product_category = random.choices(product_categories, weights=category_weights, k=1)[0]

    base_time = START_DATE + timedelta(
        days=random.randint(0, 89),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

    # Step 1: everyone in dataset views a product
    records.append({
        "user_id": user_id,
        "session_id": session_id,
        "event_time": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": "product_view",
        "device_type": device_type,
        "traffic_source": traffic_source,
        "region": region,
        "product_category": product_category,
        "order_value": None
    })

    # Base step probabilities
    p_add_to_cart = 0.34
    p_checkout = 0.62
    p_purchase = 0.48

    # Device adjustments
    if device_type == "Mobile":
        p_add_to_cart += 0.03
        p_checkout -= 0.05
        p_purchase -= 0.04
    elif device_type == "Desktop":
        p_checkout += 0.03
        p_purchase += 0.04
    elif device_type == "Tablet":
        p_add_to_cart += 0.01

    # Traffic source adjustments
    if traffic_source == "Direct":
        p_add_to_cart += 0.03
        p_checkout += 0.02
    elif traffic_source == "Organic Search":
        p_add_to_cart += 0.02
    elif traffic_source == "Paid Search":
        p_checkout -= 0.02
        p_purchase -= 0.02
    elif traffic_source == "Email":
        p_purchase += 0.03
    elif traffic_source == "Social":
        p_add_to_cart -= 0.02
        p_purchase -= 0.02

    # Region adjustments
    if region == "East":
        p_checkout += 0.02
    elif region == "West":
        p_purchase += 0.02
    elif region == "Central":
        p_checkout -= 0.03
        p_purchase -= 0.03

    # Clamp probabilities
    p_add_to_cart = max(0.05, min(p_add_to_cart, 0.95))
    p_checkout = max(0.05, min(p_checkout, 0.95))
    p_purchase = max(0.05, min(p_purchase, 0.95))

    added_to_cart = random.random() < p_add_to_cart
    started_checkout = False
    purchased = False

    if added_to_cart:
        t2 = base_time + timedelta(minutes=random.randint(1, 60))
        records.append({
            "user_id": user_id,
            "session_id": session_id,
            "event_time": t2.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": "add_to_cart",
            "device_type": device_type,
            "traffic_source": traffic_source,
            "region": region,
            "product_category": product_category,
            "order_value": None
        })

        started_checkout = random.random() < p_checkout
        if started_checkout:
            t3 = t2 + timedelta(minutes=random.randint(1, 45))
            records.append({
                "user_id": user_id,
                "session_id": session_id,
                "event_time": t3.strftime("%Y-%m-%d %H:%M:%S"),
                "event_type": "checkout_start",
                "device_type": device_type,
                "traffic_source": traffic_source,
                "region": region,
                "product_category": product_category,
                "order_value": None
            })

            purchased = random.random() < p_purchase
            if purchased:
                t4 = t3 + timedelta(minutes=random.randint(1, 30))
                order_value = round(random.uniform(20, 300), 2)
                records.append({
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_time": t4.strftime("%Y-%m-%d %H:%M:%S"),
                    "event_type": "purchase",
                    "device_type": device_type,
                    "traffic_source": traffic_source,
                    "region": region,
                    "product_category": product_category,
                    "order_value": order_value
                })

df = pd.DataFrame(records)
df = df.sort_values(["user_id", "event_time"]).reset_index(drop=True)
df.to_csv("data/ecommerce_user_events.csv", index=False)

print("Dataset generated successfully.")
print(f"Rows: {len(df)}")
print(f"Users: {df['user_id'].nunique()}")
print("Event distribution:")
print(df["event_type"].value_counts())
