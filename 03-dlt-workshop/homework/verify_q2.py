"""
Verify Question 2: What proportion of trips are paid with credit card?

Usage (requires internet):
    uv run python3 verify_q2.py

Or if taxi_data_all.json exists (from fetch_and_save.py):
    uv run python3 verify_q2.py --offline
"""
import json
import os
import sys
import ssl

# Fix SSL certificate issue on macOS
ssl_ctx = ssl.create_default_context()
try:
    import certifi
    ssl_ctx.load_verify_locations(certifi.where())
except ImportError:
    ssl_ctx = ssl._create_unverified_context()


def from_api():
    """Fetch all pages from API and compute credit card proportion."""
    import urllib.request
    
    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    page = 1
    total = 0
    credit = 0
    payment_types = {}
    
    while True:
        url = f"{base_url}?page={page}"
        print(f"Fetching page {page}...")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as response:
                raw = response.read().decode()
                data = json.loads(raw)
        except Exception as e:
            print(f"Error: {e}")
            print("No internet. Try: uv run python3 verify_q2.py --offline")
            return
        
        if not data:
            break
        
        for row in data:
            total += 1
            pt = row.get("Payment_Type", "").strip()
            payment_types[pt] = payment_types.get(pt, 0) + 1
            if pt.lower() == "credit":
                credit += 1
        
        page += 1
    
    print_results(total, credit, payment_types)


def from_file():
    """Load from taxi_data_all.json and compute credit card proportion."""
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "taxi_data_all.json")
    if not os.path.exists(filepath):
        print(f"ERROR: {filepath} not found!")
        print("Run first: uv run python3 fetch_and_save.py")
        return
    
    with open(filepath) as f:
        data = json.load(f)
    
    total = len(data)
    credit = 0
    payment_types = {}
    
    for row in data:
        pt = row.get("Payment_Type", "").strip()
        payment_types[pt] = payment_types.get(pt, 0) + 1
        if pt.lower() == "credit":
            credit += 1
    
    print_results(total, credit, payment_types)


def print_results(total, credit, payment_types):
    print("\n" + "=" * 60)
    print("QUESTION 2: Credit Card Payment Proportion")
    print("=" * 60)
    
    print(f"\nTotal trips: {total}")
    print(f"Credit card trips: {credit}")
    print(f"Proportion: {credit/total:.4f} ({credit/total*100:.2f}%)")
    
    print("\n--- All Payment Types ---")
    for pt, count in sorted(payment_types.items(), key=lambda x: -x[1]):
        print(f"  {pt:15s}: {count:6d} ({count/total*100:.2f}%)")


if __name__ == "__main__":
    if "--offline" in sys.argv:
        from_file()
    else:
        from_api()
