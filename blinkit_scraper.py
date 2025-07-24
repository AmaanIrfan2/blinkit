import pandas as pd
import requests
import time
from datetime import datetime

ACCESS_TOKEN = "v2::65b279ad-5e19-4a34-9813-1992dd7646e1"
AUTH_KEY = "Yc761ec3633c22afad934fb17a66385c1c06c5472b4898b866b7306186d0bb477"

BASE_URL = "https://blinkit.com/v1/layout/listing_widgets"

HEADERS_TEMPLATE = {
    "app_client": "consumer_web",
    "app_version": "1010101010",
    "auth_key": AUTH_KEY,
    "access_token": ACCESS_TOKEN,
    "platform": "mobile_web",
    "user-agent": "Mozilla/5.0",
    "content-type": "application/json"
}

locations = pd.read_csv("blinkit_locations.csv")
categories = pd.read_csv("blinkit_categories.csv")

FIELDNAMES = [
    "date", "l1_category", "l1_category_id", "l2_category", "l2_category_id",
    "store_id", "variant_id", "variant_name", "group_id",
    "selling_price", "mrp", "in_stock", "inventory", "is_sponsored",
    "image_url", "brand_id", "brand"
]

def scrape_for_location_category(lat, lon, l1_cat_id, l2_cat_id, l1_cat, l2_cat):
    headers = HEADERS_TEMPLATE.copy()
    headers["lat"] = str(lat)
    headers["lon"] = str(lon)
    params = {
        "l0_cat": int(l1_cat_id),
        "l1_cat": int(l2_cat_id)
    }
    try:
        r = requests.post(BASE_URL, headers=headers, params=params, json={})
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"Request error: {e}")
        return []

    results = []
    snippets = data.get("response", {}).get("snippets", [])
    for snippet in snippets:
        cart_item = (
            snippet.get("data", {})
            .get("atc_action", {})
            .get("add_to_cart", {})
            .get("cart_item", {})
        )
        if cart_item:
            result = {
                "date": datetime.now().strftime('%Y-%m-%d'),
                "l1_category": l1_cat,
                "l1_category_id": l1_cat_id,
                "l2_category": l2_cat,
                "l2_category_id": l2_cat_id,
                "store_id": cart_item.get("merchant_id"),
                "variant_id": cart_item.get("product_id"),
                "variant_name": cart_item.get("product_name", cart_item.get("display_name")),
                "group_id": cart_item.get("group_id"),
                "selling_price": cart_item.get("price"),
                "mrp": cart_item.get("mrp"),
                "in_stock": 1 if cart_item.get("inventory", 0) > 0 else 0,
                "inventory": cart_item.get("inventory"),
                "is_sponsored": 0,  # set to 1 if you later find an ad marker
                "image_url": cart_item.get("image_url"),
                "brand_id": "",
                "brand": cart_item.get("brand"),
            }
            results.append(result)
    return results

all_data = []
for _, loc in locations.iterrows():
    for _, cat in categories.iterrows():
        print(f"Scraping {cat.l1_category} > {cat.l2_category} @ ({loc.latitude},{loc.longitude})")
        batch = scrape_for_location_category(
            lat=loc.latitude,
            lon=loc.longitude,
            l1_cat_id=cat.l1_category_id,
            l2_cat_id=cat.l2_category_id,
            l1_cat=cat.l1_category,
            l2_cat=cat.l2_category
        )
        all_data.extend(batch)
        time.sleep(2)  

df = pd.DataFrame(all_data)
if len(df) > 0:
    df = df[FIELDNAMES]
    df.to_csv("blinkit_scrape_output.csv", index=False)
    print("Scrape complete!")
else:
    print("No data scraped!")