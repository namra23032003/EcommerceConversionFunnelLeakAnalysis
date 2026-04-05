import pandas as pd
import math

# Load dataset
df = pd.read_csv("data/ecommerce_user_events.csv")

# Count funnel stages
product_views = (df["event_type"] == "product_view").sum()
purchases = (df["event_type"] == "purchase").sum()

# Conversion rate
conversion_rate = purchases / product_views

# 95% confidence interval
z = 1.96
standard_error = math.sqrt((conversion_rate * (1 - conversion_rate)) / product_views)

lower = conversion_rate - z * standard_error
upper = conversion_rate + z * standard_error

print("Product Views:", product_views)
print("Purchases:", purchases)
print("Conversion Rate:", round(conversion_rate * 100, 2), "%")
print("95% Confidence Interval:")
print(round(lower * 100, 2), "% to", round(upper * 100, 2), "%")
